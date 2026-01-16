from core.base_client import HttpClient

class SupplierChatService:
    CREAT_URL='https://mapi.toysbear.net/im/ChatGroup/CreateGroup'
    SEND_URL="https://mapi.toysbear.net/im/IMChat/GroupChatSend"

    def __init__(self):
        self.http=HttpClient()
        self.logger=self.http.logger

    def create_group(self,company_number):
        json_data = {
            'userId': '19b93b3b-1238-4886-91e1-cb08de9f2fe8',
            'companyNumber': company_number,
            'chatType': 1,
        }
        res = self.http.post(self.CREAT_URL, json=json_data)
        return res['result']['item']

    def send_text(self, target_id, text,type):
        data = {
            'msgType': type,
            'content': text,
            'targetId': target_id,
            'chatType': 1
        }
        self.http.post(self.SEND_URL, json=data)
        self.logger.info(f'{type}--已发送')



# if __name__ == '__main__':
#     s=SupplierChatService()
#     target_id=s.create_group('HS162675497656148')
#     s.send_text(target_id,'你好','RC:TxtMsg')