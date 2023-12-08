import requests
import pprint

class NaverAPI:
    def __init__(self, cid, cecret):
        self.client_id = cid
        self.client_secret = cecret

    def search_cafe_posting(self, keyword):
        self.keyword = keyword
        self.url = 'https://openapi.naver.com/v1/search/cafearticle.json'

        headers = {
            'X-Naver-Client-Id': self.client_id,
            'X-Naver-Client-Secret': self.client_secret}
        params = {'query': self.keyword,
                  'start': 1,
                  'display': 100,
                  "sort": "date"}  # start: 검색 시작위치(1, 최대 100) display : 검색 결과의 개수 설정, sort : 정확도(sim), 날짜순(date)

        # API 요청 보내기
        response = requests.get(self.url, headers=headers, params=params)
        data = response.json()
        return data


if __name__ == "__main__":
    #  https://developers.naver.com/apps/#/list 어플리케이션 생성 후 발급
    id = ""
    secret_key = ""

    keyword = ""

    naver = NaverAPI(id, secret_key)
    data = naver.search_cafe_posting(keyword)
    pprint("data : ", data)
