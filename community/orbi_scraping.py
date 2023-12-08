import os, time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pprint
now = datetime.now()
today = now.strftime('%Y-%m-%d')
cat = ""
present_path = os.getcwd()
downloads_path = os.path.join(present_path, cat) # /opt/airflow

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36', # version 탭에서 스크랩
    'Accept-Language': "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6,zh;q=0.5",
    'Referer': "",
    'Accept-Encoding': "gzip, deflate, br",
}

class Orbi:
    def __init__(self):
        self.hi = "hello"
        self.url = "https://orbi.kr"
        # self.page_num = 0
        # self.page = fr"&type=keyword&page={self.page_num}"

    def estimate_reading_time(self, text):
        average_speed_per_minute = 850

        # 텍스트의 길이 측정
        text_length = len(text)

        # 읽는 데 걸리는 시간을 분 단위로 계산
        minutes = text_length / average_speed_per_minute

        # 결과를 초 단위로 변환
        seconds = minutes * 60

        return int(seconds)

    def collect_posting(self, keyword):
        islastposting = True
        data = []
        page = 1
        next_page = ""

        while islastposting and page < 5:  # 최대 80페이지 까지 가능
            islastposting = False

            searching_response = requests.get(self.url + "/search?q=" + keyword + next_page, headers=headers)
            searching_html = searching_response.text
            searching_soup = BeautifulSoup(searching_html, 'html.parser')

            posting_list_element = searching_soup.find(class_="post-list")

            # 게시물 링크 저장  // 저장하고 페이지 넘기는 것도 해야함.

            # 게시물 링크들
            link_list = []
            li_tags = posting_list_element.find_all('li')
            for idx, li_tag in enumerate(li_tags):
                p_tag = li_tag.find('p')

                posting_date = p_tag.find("abbr").get("title").replace("@", "").split()[0]
                # 오늘 날짜와 동일한 날짜의 게시물만 추리기
                if today == posting_date:  # '2023-11-03'
                    a_tag = p_tag.find('a')
                    link = a_tag['href']
                    link_list.append(link)

                    # 20번째(마지막) 게시물의 날짜가 오늘날짜와 같다 == 다음 페이지에도 오늘날짜 게시물이 있을 것으로 보인다
                    if idx == len(li_tags)-1:
                        islastposting = True

                    # 검색한 키워드가 오늘자 마지막 게시물인 경우
                else:
                    islastposting = False

            # 링크 접속 후 본문 스크랩
            for link in link_list:
                args = {}

                posting_response = requests.get(self.url + link, headers=headers)
                posting_html = posting_response.text
                posting_soup = BeautifulSoup(posting_html, 'html.parser')

                posting_num = posting_soup.find(class_="canonical clipboard tooltipped tooltipped-n").get_text().split("/")[-1].strip()
                if posting_soup.find(class_="onews-info-wrap"):  # 뉴스기사 게시물
                    posted_date = posting_soup.find(class_="onews-info-wrap").find("span").get_text().strip()
                    writer = posting_soup.find(class_="author-notice").find("a").text.split("(")[0]  # 임시

                elif posting_soup.find(class_="author-wrap").find("dt"):  # 일반게시물
                    posted_date = posting_soup.find(class_="author-wrap").find("dt").get_text().strip()
                    writer = posting_soup.find(class_="author-wrap").find(class_="nickname").find_all("span")[1].text

                posting_title = posting_soup.find(class_="title")
                if posting_title is None:
                    print("Title is None")
                else:
                    posting_title = posting_soup.find(class_="title").get_text()
                    # print("Title : ", posting_title.strip())

                posting_content = posting_soup.find(class_="content-wrap")
                if posting_content is None:
                    print("Content is None")
                else:
                    posting_content = posting_soup.find(class_="content-wrap").get_text()
                    # print("Content :", posting_content.strip())

                args["커뮤니티이름"] = "오르비"
                args["게시글번호"] = posting_num
                args["게시물등록일"] = posted_date
                args["게시글제목"] = posting_title
                args["게시글내용"] = posting_content
                args["게시글작성자"] = writer
                args["도메인"] = self.url + link

                time.sleep(self.estimate_reading_time(posting_content))  # 차단 방지
                # time.sleep(random.randint(3, 15))  # 차단 방지
                data.append(args)

            page+=1
            print("orbi page value : ", page)
            next_page = f"&type=keyword&page={page}"

            time.sleep(1)

        return data


if __name__ == "__main__":
    keyword = ""
    orbi = Orbi()
    data = orbi.collect_posting(keyword)
    pprint("data : ", data)
