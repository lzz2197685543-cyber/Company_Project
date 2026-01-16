import time

from PIL import Image
import io
import requests
from pathlib import Path
from core.get_utoken import get_utoken
from core.base_client import HttpClient

img_DIR = Path(__file__).resolve().parent.parent.parent / 'data' / 'img'
# 如果目录不存在就创建它
img_DIR.mkdir(parents=True, exist_ok=True)

class XiaozhuxiongSearch:
    URL = 'https://mapi.toysbear.net/product/TsbProduct/ProductSearchByPicture'
    IMG_URL='https://api.toysbear.net/file/File/MessageUploadFile'


    def __init__(self):
        self.http = HttpClient()
        self.logger=self.http.logger

    def search_by_image(self, image_path: str, limit=10):
        with open(image_path, 'rb') as f:
            files = {'file': ('blob', f.read(), 'image/png')}

        res = self.http.post(self.URL, files=files)
        upload_img_url=self.get_img_url(image_path)

        return self._parse(res, limit,upload_img_url),upload_img_url

    def _parse(self, res, limit,search_image):
        items = []

        for i in res['result']['item'][:limit]:
            item_dict = {
                '平台':'小竹熊',
                "搜图图片": search_image,
                "商品名称": i['name'],
                "商品图片链接": i['imageUrl'],
                "价格": i['price'],
                "供应商": i['supplierInfo']['name'],
                "联系人": i['supplierInfo'].get('contactsMan', ''),
                "手机号": i['supplierInfo']['phone'],
                'QQ':i['supplierInfo']['qq'],
                "地址": i['supplierInfo']['address'],
                '爬取数据时间':int(time.time()*1000),
                "companyNumber": i['companyNumber'],
            }
            print(item_dict)
            items.append(item_dict)
        return items

    # 使用BytesIO确保文件数据正确
    def prepare_image_data(self,file_path, max_size_kb=15):
        # 打开并压缩图片
        img = Image.open(file_path)

        # 转换为RGB模式（如果原始是RGBA）
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'RGBA':
                background.paste(img, mask=img.split()[3])  # 使用alpha通道作为mask
            else:
                background.paste(img)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')

        # 压缩图片
        img_byte_arr = io.BytesIO()
        quality = 85

        # 逐步降低质量直到文件大小符合要求
        for q in range(quality, 10, -10):
            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format='JPEG', quality=q, optimize=True)
            if len(img_byte_arr.getvalue()) <= max_size_kb * 1024:
                break

        # 如果仍然太大，调整尺寸
        if len(img_byte_arr.getvalue()) > max_size_kb * 1024:
            # 缩小图片尺寸
            new_width = min(img.width, 800)
            new_height = int(img.height * (new_width / img.width))
            img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format='JPEG', quality=70, optimize=True)

        return img_byte_arr.getvalue()

    def get_img_url(self,file_path):
        self.logger.info('正在处理我们要发送的图片')
        image_data = self.prepare_image_data(file_path)
        # print(f"图片大小: {len(image_data)} 字节")

        # 正确构建files参数
        files = [
            ('BusinessType', (None, 'RCImgMsg')),
            ('file', ('image.jpg', image_data, 'image/jpeg'))
        ]
        res=self.http.post(self.IMG_URL, files=files)
        upload_img_url=res["result"]["object"][0]['fullPath']
        return upload_img_url


if __name__ == '__main__':
    main = XiaozhuxiongSearch()
    main.search_by_image(r'D:\sd14\Factory_sourcing\data\img\basketball.png')
