import json
import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

class NewsArticle:
    def __init__(self, createdAt, title, body, subtitle, thumbnailImageUrl=None):
        self.createdAt = createdAt
        self.title = title
        self.body = body
        self.subtitle = subtitle
        self.thumbnailImageUrl = thumbnailImageUrl

    def __repr__(self):
        return (
            f"------------\n"
            f"Title: {self.title}\n"
            f"Created At: {self.createdAt}\n"
            f"Subtitle: {self.subtitle}\n"
            f"Body: {self.body}\n"
            f"Thumbnail Image URL: {self.thumbnailImageUrl}\n"
            f"------------\n"
        )

def getSecret():
    client = boto3.client("secretsmanager")
    try:
        secretValue = client.get_secret_value(SecretId="daepiro")
    except ClientError as e:
        raise Exception(f"Failed to retrieve secret: {e}")
    return json.loads(secretValue["SecretString"])

def convert_created_at_to_iso(createdAt_str):
    """createdAt 값을 ISO 8601 형식으로 변환 (밀리초 없이)"""
    # 현재 연도를 가져옴
    current_year = datetime.now().year
    # 주어진 'MM-DD HH:MM' 형식의 문자열을 datetime으로 파싱
    parsed_datetime = datetime.strptime(f"{current_year}-{createdAt_str}", "%Y-%m-%d %H:%M")
    # UTC로 변환하고 밀리초 없이 ISO 8601 형식으로 변환
    return parsed_datetime.strftime('%Y-%m-%dT%H:%M:%S')

def parse_published_at(published_at_str):
    """밀리초 없이 ISO 8601 날짜 형식을 처리"""
    return datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%S")

def get_latest_published_at(api_url, headers):
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # 오류가 있으면 예외 발생
        data = response.json()
        print(f"API Response Data: {data}")
        return data['data']['publishedAt']  # 가장 최근의 publishedAt 반환
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch latest publishedAt: {e}")
        return None

def lambda_handler(event, context):
    secret = getSecret()

    # Authorization 헤더에 ADMIN_ACCESS_TOKEN 추가
    headers = {
        "Authorization": f"Bearer {secret['ADMIN_ACCESS_TOKEN']}",
        "Content-Type": "application/json"
    }

    # API 서버에서 최근 뉴스 정보 가져오기
    get_latest_url = f"{secret['API_SERVER_BASE_URL']}/v1/datacollector/news/latest"
    latest_published_at = get_latest_published_at(get_latest_url, headers)
    
    if latest_published_at:
        latest_published_at_dt = parse_published_at(latest_published_at)
        print(f"Latest publishedAt from API: {latest_published_at_dt}")
    else:
        print("Unable to retrieve the latest publishedAt, no filtering will be applied.")
        latest_published_at_dt = None

    # 뉴스 데이터를 가져오는 부분
    response = requests.get(secret["DISASTER_NEWS_URL"])
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    articles = soup.find_all('article')
    newsArticles = []
    for article in articles:
        createdAt = article.select_one('span.tt').get_text(strip=True)
        titleTag = article.select_one('h3.tit-news a')
        title = titleTag.get_text(strip=True)
        body = titleTag['href']
        subtitle = article.select_one('p.lead').get_text(strip=True)
        thumbnailImageUrl = None
        figureTag = article.select_one('figure.img-con')
        if figureTag:
            thumbnailImageUrl = figureTag.find('img').get('src')
        newsArticle = NewsArticle(
            createdAt=createdAt,
            title=title,
            body=body,
            subtitle=subtitle,
            thumbnailImageUrl=thumbnailImageUrl
        )
        newsArticles.append(newsArticle)

    for article in newsArticles:
        print(article)

    # 필터링: latest_published_at보다 최신인 뉴스만 선택
    if latest_published_at_dt:
        filtered_news_articles = [
            article for article in newsArticles 
            if parse_published_at(convert_created_at_to_iso(article.createdAt)) > latest_published_at_dt
        ]
    else:
        filtered_news_articles = newsArticles

    # POST 요청을 위한 뉴스 데이터를 JSON 형식으로 변환
    news_data = {
        "news": [
            {
                "title": article.title,
                "publishedAt": convert_created_at_to_iso(article.createdAt),
                "subtitle": article.subtitle,
                "body": article.body,
                "thumbnailUrl": article.thumbnailImageUrl
            }
            for article in filtered_news_articles
        ]
    }

    if not news_data["news"]:
        print("No new articles to send.")
        return {'statusCode': 204, 'body': "No new articles to send."}

    # API 서버에 POST 요청 보내기
    save_news_url = f"{secret['API_SERVER_BASE_URL']}/v1/datacollector/news"
    
    try:
        api_response = requests.post(save_news_url, json=news_data, headers=headers)
        api_response.raise_for_status()  # HTTP 오류가 발생하면 예외를 던짐
        print(f"Successfully sent data to {save_news_url}. Response: {api_response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send data: {e}")

    # 요청을 보낸 후 news_data 출력
    print("Sent news data:")
    print(json.dumps(news_data, indent=4, ensure_ascii=False))  # JSON 형식으로 보기 좋게 출력

    return {
        'statusCode': 200
    }

#lambda_handler(None, None)
