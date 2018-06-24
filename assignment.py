from flask import Flask, render_template, request, redirect
import xlrd
import json
import boto3
import hashlib
import time

from werkzeug.wsgi import LimitedStream


class StreamConsumingMiddleware(object):

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        stream = LimitedStream(environ['wsgi.input'], int(environ.get('CONTENT_LENGTH', 0) or 0))
        environ['wsgi.input'] = stream
        app_iter = self.app(environ, start_response)
        try:
            stream.exhaust()
            for event in app_iter:
                yield event
        finally:
            if hasattr(app_iter, 'close'):
                app_iter.close()

app=Flask(__name__)
@app.route('/')
def upload_file():
    return render_template('upload.html')


@app.route('/assignment', methods=['POST'])
def assignment():
    if request.method == "POST":
        try:
            f = request.files['file']
        except KeyError as e:
            return json.dumps({"status":"Failed","reason":"No file uploaded"})

        temp_list = []
        list_of_entries = []
        try:
            wb = xlrd.open_workbook(file_contents=f.read())
        except xlrd.biffh.XLRDError as e:
            return json.dumps({"status":"Failed","reason":"File format should be xls"})
        sheet_number = 1
        try:
            sheet = wb.sheet_by_index(sheet_number)
        except IndexError as e:
            return json.dumps({"status": "Failed", "reason": "Sheet #%d does not exist" % (sheet_number, )})
        sheet.cell_value(0, 0)

        for i in range(sheet.ncols):
            col_val = sheet.cell_value(0, i)
            temp_list.append(col_val)

        for i in range(1, sheet.nrows):
            d = {}
            row_val = sheet.row_values(i)
            for j in range(sheet.ncols):
                d[temp_list[j]] = row_val[j]
            list_of_entries.append(d)

        # Unique file name based on timestamp
        file_name = hashlib.md5(str(time.time()).encode('utf-8')).hexdigest() + '.json'
        json_file = json.dumps(list_of_entries)
        s3 = boto3.resource('s3')
        object = s3.Object('steeleye', file_name)
        object.put(Body=json_file)
        bucket_url = 'https://s3.ap-south-1.amazonaws.com/steeleye/'
        return json.dumps({"file_name": bucket_url + file_name, "status": "Success"})

if __name__=='__main__':
    app.wsgi_app = StreamConsumingMiddleware(app.wsgi_app)
    app.run(debug=True)
