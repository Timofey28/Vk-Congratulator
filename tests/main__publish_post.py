from main import publish_post

message = 'Тест'
attachment = ''
link = publish_post(message=message, attachment=attachment)
print(link)