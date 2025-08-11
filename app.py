"""
关键词统计系统 - 在线部署版本
优化了在线部署的配置和性能
"""

from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename
import os
import json
import uuid
from datetime import datetime
import threading
import time
import pandas as pd
from keyword_processor import WebKeywordMatcher

# 创建Flask应用
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'keyword-system-online-2024')

# 配置
UPLOAD_FOLDER = 'uploads'
RESULTS_FOLDER = 'results'
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

# 确保文件夹存在
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
os.makedirs('templates', exist_ok=True)

# 全局变量存储处理状态
processing_status = {}

def allowed_file(filename):
    """检查文件类型是否允许"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def process_keywords_async(task_id):
    """异步处理关键词匹配"""
    try:
        status = processing_status[task_id]
        status['status'] = 'processing'
        status['progress'] = 10
        status['message'] = '正在初始化...'
        
        # 创建关键词匹配器
        matcher = WebKeywordMatcher(
            keyword_file=status['keyword_file'],
            options=status['options']
        )
        
        status['progress'] = 30
        status['message'] = '词库加载完成，开始处理...'
        
        # 生成输出文件名
        output_filename = f"{task_id}_result.xlsx"
        output_path = os.path.join(RESULTS_FOLDER, output_filename)
        
        # 定义进度回调函数
        def progress_callback(progress):
            if task_id in processing_status:
                processing_status[task_id]['progress'] = 30 + int(progress * 0.6)
                processing_status[task_id]['message'] = f'处理中... {progress}%'
        
        # 处理文件
        result = matcher.process_file(
            input_file=status['phrase_file'],
            output_file=output_path,
            progress_callback=progress_callback
        )
        
        if result['success']:
            status['status'] = 'completed'
            status['progress'] = 100
            status['message'] = '处理完成'
            status['output_file'] = output_path
            status['result_data'] = result
            status['end_time'] = datetime.now()
        else:
            status['status'] = 'error'
            status['error'] = result['error']
            status['message'] = f'处理失败: {result["error"]}'
            
    except Exception as e:
        if task_id in processing_status:
            processing_status[task_id]['status'] = 'error'
            processing_status[task_id]['error'] = str(e)
            processing_status[task_id]['message'] = f'处理失败: {str(e)}'

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    """文件上传接口"""
    try:
        # 检查是否有文件
        if 'keyword_file' not in request.files or 'phrase_file' not in request.files:
            return jsonify({'error': '请选择词库文件和短语文件'}), 400
        
        keyword_file = request.files['keyword_file']
        phrase_file = request.files['phrase_file']
        
        # 检查文件名
        if keyword_file.filename == '' or phrase_file.filename == '':
            return jsonify({'error': '请选择有效的文件'}), 400
        
        # 检查文件类型
        if not (allowed_file(keyword_file.filename) and allowed_file(phrase_file.filename)):
            return jsonify({'error': '只支持Excel和CSV文件(.xlsx, .xls, .csv)'}), 400
        
        # 生成唯一的任务ID
        task_id = str(uuid.uuid4())
        
        # 保存文件
        keyword_filename = secure_filename(f"{task_id}_keyword_{keyword_file.filename}")
        phrase_filename = secure_filename(f"{task_id}_phrase_{phrase_file.filename}")
        
        keyword_path = os.path.join(UPLOAD_FOLDER, keyword_filename)
        phrase_path = os.path.join(UPLOAD_FOLDER, phrase_filename)
        
        keyword_file.save(keyword_path)
        phrase_file.save(phrase_path)
        
        # 获取处理选项
        options = {
            'rank_limit': request.form.get('rank_limit', ''),
            'custom_stopwords': request.form.get('custom_stopwords', ''),
            'include_stats': request.form.get('include_stats') == 'true',
            'highlight_cells': request.form.get('highlight_cells') == 'true',
            'export_excel': request.form.get('export_excel') == 'true'
        }
        
        # 初始化处理状态
        processing_status[task_id] = {
            'status': 'uploaded',
            'progress': 0,
            'message': '文件上传成功',
            'keyword_file': keyword_path,
            'phrase_file': phrase_path,
            'options': options,
            'start_time': datetime.now(),
            'result_files': []
        }
        
        # 启动异步处理
        thread = threading.Thread(target=process_keywords_async, args=(task_id,))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': '文件上传成功，开始处理'
        })
        
    except Exception as e:
        return jsonify({'error': f'上传失败: {str(e)}'}), 500

@app.route('/progress/<task_id>')
def get_progress(task_id):
    """获取处理进度"""
    if task_id not in processing_status:
        return jsonify({'error': '任务不存在'}), 404
    
    status = processing_status[task_id]
    return jsonify({
        'task_id': task_id,
        'status': status['status'],
        'progress': status['progress'],
        'message': status['message'],
        'error': status.get('error', '')
    })

@app.route('/result/<task_id>')
def get_result(task_id):
    """获取处理结果"""
    if task_id not in processing_status:
        return jsonify({'error': '任务不存在'}), 404
    
    status = processing_status[task_id]
    
    if status['status'] != 'completed':
        return jsonify({'error': '任务尚未完成'}), 400
    
    try:
        result_data = status.get('result_data', {})
        
        # 读取结果文件的前几行作为预览
        preview_data = []
        if 'output_file' in status and os.path.exists(status['output_file']):
            try:
                df = pd.read_excel(status['output_file'])
                preview_data = df.head(10).to_dict('records')
            except Exception as e:
                print(f"读取预览数据失败: {e}")
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'statistics': result_data.get('statistics', {}),
            'total_processed': result_data.get('total_processed', 0),
            'preview': preview_data,
            'download_url': f'/download/{task_id}',
            'processing_time': str(status.get('end_time', datetime.now()) - status['start_time'])
        })
        
    except Exception as e:
        return jsonify({'error': f'获取结果失败: {str(e)}'}), 500

@app.route('/download/<task_id>')
def download_result(task_id):
    """下载结果文件"""
    if task_id not in processing_status:
        return jsonify({'error': '任务不存在'}), 404
    
    status = processing_status[task_id]
    
    if status['status'] != 'completed':
        return jsonify({'error': '任务尚未完成'}), 400
    
    if 'output_file' not in status or not os.path.exists(status['output_file']):
        return jsonify({'error': '结果文件不存在'}), 404
    
    try:
        return send_file(
            status['output_file'],
            as_attachment=True,
            download_name=f'关键词分析结果_{task_id[:8]}.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        return jsonify({'error': f'下载失败: {str(e)}'}), 500

@app.route('/health')
def health_check():
    """健康检查接口"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'active_tasks': len(processing_status)
    })

@app.errorhandler(413)
def too_large(e):
    return jsonify({'error': '文件过大，请选择小于50MB的文件'}), 413

@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': '页面不存在'}), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({'error': '服务器内部错误'}), 500

# 获取端口号（适配云平台）
port = int(os.environ.get('PORT', 5000))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=False)