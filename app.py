import os
import zipfile
import pandas as pd
import re
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_from_directory
from werkzeug.utils import secure_filename
import shutil
import json

app = Flask(__name__)
app.secret_key = 'datashow_secret_key'

# 配置上传文件夹
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
EXTRACT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'extracted')
ALLOWED_EXTENSIONS = {'zip'}

# 确保上传和解压目录存在
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(EXTRACT_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['EXTRACT_FOLDER'] = EXTRACT_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 限制上传文件大小为16MB

# 合规性规则
COMPLIANCE_RULES = {
    'USB': {
        'rule': 'No unauthorized USB devices',
        'check': lambda df: df[df['Description'].str.contains('Unauthorized', case=False, na=False)] if 'Description' in df.columns else pd.DataFrame()
    },
    'DISK': {
        'rule': 'All disks must be encrypted',
        'check': lambda df: df[df['Encrypted'].str.contains('No', case=False, na=False)] if 'Encrypted' in df.columns else pd.DataFrame()
    },
    'DEV': {
        'rule': 'No development tools on production machines',
        'check': lambda df: df[df['Type'].str.contains('Development', case=False, na=False)] if 'Type' in df.columns else pd.DataFrame()
    }
    # 可以根据实际需求添加更多规则
}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_csv_type(filename):
    # 从文件名中提取类型（如USB、DISK等）
    match = re.match(r'^([A-Z]+)_', os.path.basename(filename))
    if match:
        return match.group(1)
    return None

def check_compliance(df, csv_type):
    if csv_type in COMPLIANCE_RULES:
        rule = COMPLIANCE_RULES[csv_type]
        violations = rule['check'](df)
        return {
            'compliant': len(violations) == 0,
            'rule': rule['rule'],
            'violations': violations.to_dict('records') if not violations.empty else []
        }
    return {'compliant': True, 'rule': 'No specific rules for this type', 'violations': []}

@app.route('/')
def index():
    # 获取已上传的文件列表
    uploaded_files = []
    for folder in os.listdir(EXTRACT_FOLDER):
        folder_path = os.path.join(EXTRACT_FOLDER, folder)
        if os.path.isdir(folder_path):
            csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
            uploaded_files.append({
                'name': folder,
                'files': csv_files
            })
    
    return render_template('index.html', uploaded_files=uploaded_files)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        # 创建解压目录
        extract_dir = os.path.join(app.config['EXTRACT_FOLDER'], filename.rsplit('.', 1)[0])
        os.makedirs(extract_dir, exist_ok=True)
        
        # 解压文件
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        flash('File successfully uploaded and extracted')
        return redirect(url_for('index'))
    
    flash('Invalid file type')
    return redirect(url_for('index'))

@app.route('/view/<folder>/<filename>')
def view_file(folder, filename):
    file_path = os.path.join(app.config['EXTRACT_FOLDER'], folder, filename)
    if not os.path.exists(file_path):
        flash('File not found')
        return redirect(url_for('index'))
    
    try:
        # Try different encodings
        try:
            df = pd.read_csv(file_path, encoding='gbk')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file_path, encoding='gb2312')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='gb18030')
        
        csv_type = get_csv_type(filename)
        compliance_result = check_compliance(df, csv_type)
        
        return render_template('view.html', 
                             folder=folder,
                             filename=filename, 
                             data=df.to_html(classes='table table-striped', index=False),
                             columns=df.columns.tolist(),
                             csv_type=csv_type,
                             compliance=compliance_result)
    except Exception as e:
        flash(f'Error reading file: {str(e)}')
        return redirect(url_for('index'))

@app.route('/api/data/<folder>/<filename>')
def get_data(folder, filename):
    file_path = os.path.join(app.config['EXTRACT_FOLDER'], folder, filename)
    if not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404
    
    try:
        # Try different encodings
        try:
            df = pd.read_csv(file_path, encoding='gbk')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file_path, encoding='gb2312')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='gb18030')
        
        # Get DataTables parameters
        start = int(request.args.get('start', 0))
        length = int(request.args.get('length', 10))
        search_value = request.args.get('search[value]', '')
        
        # Apply search filter if provided
        if search_value:
            df = df[df.astype(str).apply(lambda x: x.str.contains(search_value, case=False, na=False)).any(axis=1)]
        
        total_records = len(df)
        
        # Apply pagination
        df = df.iloc[start:start + length]
        
        # Ensure all data is serializable
        df = df.fillna('')
        records = df.to_dict('records')
        
        return jsonify({
            'draw': int(request.args.get('draw', 1)),
            'recordsTotal': total_records,
            'recordsFiltered': total_records,
            'data': records
        })
        
    except Exception as e:
        return jsonify({
            'draw': int(request.args.get('draw', 1)),
            'recordsTotal': 0,
            'recordsFiltered': 0,
            'data': [],
            'error': str(e)
        })

@app.route('/api/compliance/<folder>/<filename>')
def check_file_compliance(folder, filename):
    file_path = os.path.join(app.config['EXTRACT_FOLDER'], folder, filename)
    if not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404
    
    try:
        df = pd.read_csv(file_path)
        csv_type = get_csv_type(filename)
        compliance_result = check_compliance(df, csv_type)
        return jsonify(compliance_result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/delete/<folder>', methods=['POST'])
def delete_folder(folder):
    folder_path = os.path.join(app.config['EXTRACT_FOLDER'], folder)
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        shutil.rmtree(folder_path)
        flash(f'Deleted {folder}')
    else:
        flash('Folder not found')
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)