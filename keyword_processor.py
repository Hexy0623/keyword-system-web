"""
关键词处理核心模块 - 在线部署版本
简化版本，专注核心功能，减少依赖
"""

import pandas as pd
import os
import re
from collections import Counter, defaultdict
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import tempfile

# 停用词列表
DEFAULT_STOPWORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
    'from', 'up', 'about', 'into', 'through', 'during', 'before', 'after', 'above', 'below',
    'between', 'among', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'can',
    'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'me', 'him',
    'her', 'us', 'them', 'my', 'your', 'his', 'her', 'its', 'our', 'their', 'mine', 'yours',
    'hers', 'ours', 'theirs', 'myself', 'yourself', 'himself', 'herself', 'itself', 'ourselves',
    'yourselves', 'themselves', 'what', 'which', 'who', 'whom', 'whose', 'where', 'when', 'why',
    'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no',
    'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'just', 'now'
}

def strQ2B(ustring):
    """全角转半角"""
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 12288:
            inside_code = 32
        elif 65281 <= inside_code <= 65374:
            inside_code -= 65248
        rstring += chr(inside_code)
    return rstring

class WebKeywordMatcher:
    """Web版关键词匹配器"""
    
    def __init__(self, keyword_file, options=None):
        self.options = options or {}
        self.stopwords = self._get_stopwords()
        self.rank_limit = self._parse_rank_limit()
        self._load_keyword_database(keyword_file)
        
    def _get_stopwords(self):
        """获取停用词"""
        stopwords = DEFAULT_STOPWORDS.copy()
        custom_stopwords = self.options.get('custom_stopwords', '')
        if custom_stopwords:
            custom_words = [w.strip().lower() for w in custom_stopwords.split(',') if w.strip()]
            stopwords.update(custom_words)
        return stopwords
    
    def _parse_rank_limit(self):
        """解析排名限制"""
        rank_limit_str = self.options.get('rank_limit', '')
        if rank_limit_str and rank_limit_str.isdigit():
            return int(rank_limit_str)
        return None
    
    def _load_keyword_database(self, keyword_file):
        """加载关键词数据库"""
        try:
            self.keyword_df = pd.read_excel(keyword_file)
            self.keyword_df.columns = [strQ2B(str(c)).strip().replace(' ', '').replace('（', '(').replace('）', ')') 
                                     for c in self.keyword_df.columns]
            
            # 自动映射表头
            col_map = {}
            for c in self.keyword_df.columns:
                if 'keyword' in c.lower() or '关键词' in c:
                    col_map['Keyword'] = c
                elif 'type' in c.lower() or '类型' in c:
                    col_map['Type'] = c
                elif 'gender' in c.lower() or '性别' in c:
                    col_map['Gender'] = c
                elif 'size' in c.lower() or '尺码' in c:
                    col_map['Size'] = c
                elif 'age' in c.lower() or '年龄' in c or 'specialage' in c.replace(' ', '').lower():
                    col_map['Special Age'] = c
                elif 'brand' in c.lower() or '品牌' in c:
                    col_map['Brand'] = c
            
            # 提取各字段列表
            self.keyword_list = self.keyword_df[col_map.get('Keyword', self.keyword_df.columns[0])].astype(str).fillna('').tolist()
            self.type_list = self.keyword_df[col_map.get('Type', self.keyword_df.columns[0])].astype(str).fillna('').tolist() if 'Type' in col_map else []
            self.gender_list = self.keyword_df[col_map.get('Gender', self.keyword_df.columns[0])].astype(str).fillna('').tolist() if 'Gender' in col_map else []
            self.size_list = self.keyword_df[col_map.get('Size', self.keyword_df.columns[0])].astype(str).fillna('').tolist() if 'Size' in col_map else []
            self.age_list = self.keyword_df[col_map.get('Special Age', self.keyword_df.columns[0])].astype(str).fillna('').tolist() if 'Special Age' in col_map else []
            self.brand_list = self.keyword_df[col_map.get('Brand', self.keyword_df.columns[0])].astype(str).fillna('').tolist() if 'Brand' in col_map else []
            
            # 构建索引
            self.keyword_index = self._build_index(self.keyword_list)
            self.type_index = self._build_index(self.type_list)
            self.gender_index = self._build_index(self.gender_list)
            self.size_index = self._build_index(self.size_list)
            self.age_index = self._build_index(self.age_list)
            self.brand_index = self._build_index(self.brand_list)
            
        except Exception as e:
            raise Exception(f'词库加载失败: {str(e)}')
    
    def _build_index(self, word_list):
        """构建关键词索引"""
        index = {}
        for i, word in enumerate(word_list):
            w = strQ2B(word.strip().lower())
            if not w:
                continue
            first = w.split()[0] if w.split() else w
            index.setdefault(first, []).append((w, i))
        
        for first in index:
            index[first].sort(key=lambda x: (len(x[0].split()), len(x[0])), reverse=True)
        return index

    def clean_phrase(self, phrase):
        """清洗短语"""
        if not phrase:
            return ""
        
        phrase = strQ2B(str(phrase))
        phrase = re.sub(r'[^\w\s\u4e00-\u9fff]', ' ', phrase)
        words = phrase.lower().split()
        filtered_words = [w for w in words if w not in self.stopwords and len(w) > 1]
        return ' '.join(filtered_words)

    def match_from_index(self, phrase, index, used_positions=None):
        """从索引中匹配关键词"""
        if used_positions is None:
            used_positions = set()
        
        matches = []
        words = phrase.split()
        
        for i, word in enumerate(words):
            if i in used_positions:
                continue
                
            if word in index:
                for keyword, _ in index[word]:
                    keyword_words = keyword.split()
                    if len(keyword_words) == 1:
                        if word == keyword:
                            matches.append((keyword, i, i))
                            used_positions.add(i)
                            break
                    else:
                        if i + len(keyword_words) <= len(words):
                            phrase_part = ' '.join(words[i:i+len(keyword_words)])
                            if phrase_part == keyword:
                                matches.append((keyword, i, i+len(keyword_words)-1))
                                for j in range(i, i+len(keyword_words)):
                                    used_positions.add(j)
                                break
        
        return matches, used_positions

    def extract_all(self, phrase):
        """提取所有字段信息"""
        cleaned_phrase = self.clean_phrase(phrase)
        if not cleaned_phrase:
            return {
                'keywords': [], 'brands': [], 'types': [], 'sizes': [], 
                'ages': [], 'genders': [], 'uncovered_words': []
            }
        
        used_positions = set()
        
        # 匹配各字段
        keywords, used_positions = self.match_from_index(cleaned_phrase, self.keyword_index, used_positions)
        brands, used_positions = self.match_from_index(cleaned_phrase, self.brand_index, used_positions)
        types, used_positions = self.match_from_index(cleaned_phrase, self.type_index, used_positions)
        sizes, used_positions = self.match_from_index(cleaned_phrase, self.size_index, used_positions)
        ages, used_positions = self.match_from_index(cleaned_phrase, self.age_index, used_positions)
        genders, used_positions = self.match_from_index(cleaned_phrase, self.gender_index, used_positions)
        
        # 提取未覆盖单词
        words = cleaned_phrase.split()
        uncovered_words = [words[i] for i in range(len(words)) if i not in used_positions]
        
        return {
            'keywords': [k[0] for k in keywords],
            'brands': [k[0] for k in brands],
            'types': [k[0] for k in types],
            'sizes': [k[0] for k in sizes],
            'ages': [k[0] for k in ages],
            'genders': [k[0] for k in genders],
            'uncovered_words': uncovered_words
        }

    def process_file(self, input_file, output_file, progress_callback=None):
        """处理文件的主要方法"""
        try:
            # 读取输入文件
            if input_file.endswith('.xlsx') or input_file.endswith('.xls'):
                df = pd.read_excel(input_file)
            elif input_file.endswith('.csv'):
                df = pd.read_csv(input_file)
            else:
                raise ValueError("不支持的文件格式")
            
            # 查找短语列
            phrase_col = None
            for col in df.columns:
                if any(keyword in str(col).lower() for keyword in ['phrase', '短语', 'title', '标题', 'keyword', '关键词']):
                    phrase_col = col
                    break
            
            if phrase_col is None:
                phrase_col = df.columns[0]
            
            # 应用排名限制
            if self.rank_limit and self.rank_limit > 0:
                df = df.head(self.rank_limit)
            
            # 处理每一行
            results = []
            total_rows = len(df)
            
            for idx, row in df.iterrows():
                phrase = str(row[phrase_col]) if pd.notna(row[phrase_col]) else ""
                
                # 提取关键词信息
                extracted = self.extract_all(phrase)
                
                # 构建结果行
                result_row = {
                    '原始短语': phrase,
                    '关键词': ', '.join(extracted['keywords']),
                    '品牌': ', '.join(extracted['brands']),
                    '类型': ', '.join(extracted['types']),
                    '尺码': ', '.join(extracted['sizes']),
                    '年龄': ', '.join(extracted['ages']),
                    '性别': ', '.join(extracted['genders']),
                    '未覆盖单词': ', '.join(extracted['uncovered_words'])
                }
                
                # 添加原始数据的其他列
                for col in df.columns:
                    if col != phrase_col:
                        result_row[f'原_{col}'] = row[col]
                
                results.append(result_row)
                
                # 更新进度
                if progress_callback:
                    progress = int((idx + 1) / total_rows * 100)
                    progress_callback(progress)
            
            # 保存结果
            result_df = pd.DataFrame(results)
            result_df.to_excel(output_file, index=False)
            
            # 生成统计信息
            stats = self._generate_statistics(results)
            
            return {
                'success': True,
                'output_file': output_file,
                'total_processed': total_rows,
                'statistics': stats
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def _generate_statistics(self, results):
        """生成统计信息"""
        total_phrases = len(results)
        
        # 统计各字段的覆盖率
        keyword_coverage = sum(1 for r in results if r['关键词']) / total_phrases * 100 if total_phrases > 0 else 0
        brand_coverage = sum(1 for r in results if r['品牌']) / total_phrases * 100 if total_phrases > 0 else 0
        type_coverage = sum(1 for r in results if r['类型']) / total_phrases * 100 if total_phrases > 0 else 0
        
        # 统计未覆盖单词
        all_uncovered = []
        for r in results:
            if r['未覆盖单词']:
                all_uncovered.extend(r['未覆盖单词'].split(', '))
        
        uncovered_counter = Counter(all_uncovered)
        
        return {
            'total_phrases': total_phrases,
            'keyword_coverage': round(keyword_coverage, 2),
            'brand_coverage': round(brand_coverage, 2),
            'type_coverage': round(type_coverage, 2),
            'top_uncovered_words': uncovered_counter.most_common(10)
        }