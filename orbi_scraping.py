import random
import time
import requests
from pprint import pprint
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import pandas as pd

url = "https://orbi.kr"
search_keyword = "검색키워드"
page_num = 0
page = fr"&type=keyword&page={page_num}"

sql_args = []
def calculate_comment_date(hours=0, minutes=0):
    current_datetime = datetime.now()
    delta = timedelta(hours=hours, minutes=minutes)
    calculated_datetime = current_datetime - delta
    return calculated_datetime.strftime('%Y-%m-%d %H:%M:%S')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
}

searching_response = requests.get(url + "/search?q=" + search_keyword, headers=headers)

searching_html = searching_response.text
searching_soup = BeautifulSoup(searching_html, 'html.parser')
posting_list_element = searching_soup.find(class_="post-list")

# 게시물 링크들
link_list = []
li_tags = posting_list_element.find_all('li')
for li_tag in li_tags:
    p_tag = li_tag.find('p')
    a_tag = p_tag.find('a')
    link = a_tag['href']
    link_list.append(link)

# 본문 스크랩
for link in link_list[:3]:
    args = {}

    posting_response = requests.get(url + link, headers=headers)
    posting_html = posting_response.text
    posting_soup = BeautifulSoup(posting_html, 'html.parser')

    게시글번호 = posting_soup.find(class_="canonical clipboard tooltipped tooltipped-n").get_text().split("/")[-1].strip()
    if posting_soup.find(class_="onews-info-wrap"):  # 뉴스기사 게시물
        게시물_등록일 = posting_soup.find(class_="onews-info-wrap").find("span").get_text().strip()
        게시글작성자 = posting_soup.find(class_="author-notice").find("a").text.split("(")[0]  # 임시

    elif posting_soup.find(class_="author-wrap").find("dt"):  # 일반게시물
        게시물_등록일 = posting_soup.find(class_="author-wrap").find("dt").get_text().strip()
        게시글작성자 = posting_soup.find(class_="author-wrap").find(class_="nickname").find_all("span")[1].text

    print("게시물 등록일 : ", 게시물_등록일)
    print("게시글 번호 : ", 게시글번호)

    posting_title = posting_soup.find(class_="title")
    if posting_title is None:
        print("title is None")
    else:
        posting_title = posting_soup.find(class_="title").get_text()
        print("title : ", posting_title.strip())

    posting_content = posting_soup.find(class_="content-wrap")
    if posting_content is None:
        print("content is None")
    else:
        posting_content = posting_soup.find(class_="content-wrap").get_text()
        print("content :", posting_content.strip())

    print("")

    args["커뮤니티이름"] = "orbi"
    args["게시글번호"] = 게시글번호
    args["게시물_등록일"] = 게시물_등록일
    args["게시글제목"] = posting_title
    args["게시글내용"] = posting_content
    args["게시글작성자"] = 게시글작성자
    args["댓글"] = []

    # 댓글 수집
    comment_list = posting_soup.find(class_="comment-list")
    comment_tags = comment_list.find_all("comment-box")

    for comment in comment_list.find_all(recursive=False):
        if comment.find("p") is None:  # 삭제된 댓글, 무댓글
            continue

        kwargs = {
            "닉네임": "",
            "댓글내용": "",
            "작성일": ""
        }

        닉네임 = comment.find("a").get_text()[1:-1]  # \n 을 자르기 위한 슬라이스 처리
        댓글내용 = comment.find("p").get_text()

        if "전" in comment.find("div").find(class_="meta-wrap").text.strip().split():
            # 최근 댓글
            전_idx = comment.find("div").find(class_="meta-wrap").text.strip().split().index("전")

            댓글작성상대시간 = comment.find("div").find(class_="meta-wrap").text.strip().split()[전_idx-1]
            if "시간" in 댓글작성상대시간:
                댓글작성일 = calculate_comment_date(int(댓글작성상대시간.replace("시간", "")))
            elif "분" in 댓글작성상대시간:
                댓글작성일 = calculate_comment_date(0, int(댓글작성상대시간.replace("분", "")))
        else:
            묵힌댓글년 = str(datetime.strptime(게시물_등록일, "%Y-%m-%d %H:%M:%S").year)

            댓글정보 = comment.find('div').find(class_="meta-wrap").text.split()
            묵힌댓글일 = [item for item in 댓글정보 if '/' in item or ':' in item][0].replace("/", "-")
            묵힌댓글시간 = [item for item in 댓글정보 if '/' in item or ':' in item][1]
            댓글작성일 = (묵힌댓글년 + "-" + 묵힌댓글일 + " " + 묵힌댓글시간 + ":00")

        print("댓글작성일:", 댓글작성일)
        print("닉네임 : ", 닉네임)
        print("댓글내용 : ", 댓글내용)
        print("")

        kwargs['닉네임'] = 닉네임
        kwargs['댓글내용'] = 댓글내용
        kwargs['작성일'] = 댓글작성일

        args['댓글'].append(kwargs)

    sql_args.append(args)
    print("----------------------------------게시물 끝----------------------------------")
    time.sleep(random.randint(5,15))

pprint(sql_args)

time.sleep(15)

# # 데이터 저장
# board_data = {}
# comment_data = {}
# board_data = []
# comment_data = []
# board_args = {}
# comment_args = {}
#
# for board in sql_args:
#     board_args = {
#         "커뮤니티이름": board['커뮤니티이름'],
#         "게시글작성자": board['게시글작성자'],
#         "게시글번호": board['게시글번호'],
#         "게시글등록일": board['게시물_등록일'],
#         "게시글제목": board['게시글제목'],
#         "게시글내용": board['게시글내용']
#     }
#
#     board_data.append(board_args)
#
#     for comments in board['댓글']:
#         comment_args = {
#             "커뮤니티이름": board['커뮤니티이름'],
#             "게시글번호": board['게시글번호'],
#             "작성일": comments['작성일'],
#             "닉네임": comments['닉네임'],
#             "댓글내용": comments['댓글내용']
#         }
#
#         comment_data.append(comment_args)

# json_board_data = json.dumps(board_data, ensure_ascii=False)
# json_comment_data = json.dumps(comment_data, ensure_ascii=False)
#
# df_board_data = pd.read_json(json_board_data)
# df_comment_data = pd.read_json(json_comment_data)
#
# df_board_data.to_parquet("board.parquet")
# df_comment_data.to_parquet("comment.parquet")