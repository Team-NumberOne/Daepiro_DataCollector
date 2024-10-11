import requests
import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime
import re

def preprocessing_address(address):
	return re.sub(r'\b(\w+)( \1\b)+', r'\1', address).strip()

def getSecret():
	client = boto3.client("secretsmanager")
	try:
		secretValue = client.get_secret_value(SecretId="daepiro")
	except ClientError as e:
		raise Exception(f"Failed to retrieve secret: {e}")
	return json.loads(secretValue["SecretString"])

# 재난 메시지 클래스를 정의
class DisasterMessage:
	def __init__(self, msg_cn, rcptn_rgn_nm, crt_dt, dst_se_nm, sn):
		self.msg_cn = msg_cn  # 메시지 내용
		self.rcptn_rgn_nm = rcptn_rgn_nm  # 수신 지역
		self.crt_dt = crt_dt  # 생성일시
		self.dst_se_nm = dst_se_nm  # 재난 종류
		self.sn = sn  # 일련번호 (SN)

	def __repr__(self):
		return (
			f"------------\n"
			f"Message Content: {self.msg_cn}\n"
			f"Region: {self.rcptn_rgn_nm}\n"
			f"Created Date: {self.crt_dt}\n"
			f"Disaster Type: {self.dst_se_nm}\n"
			f"SN: {self.sn}\n"
			f"------------\n"
		)

# 최신 재난 메시지 조회
def get_latest_message_id(base_url, headers):
	url = f"{base_url}/v1/datacollector/disasters/latest"
	try:
		response = requests.get(url, headers=headers)
		response.raise_for_status()
		data = response.json()

		if data["code"] == 1000:
			return data["data"]["messageId"]
		else:
			print(f"API 오류: {data['message']}")
			return None
	except requests.exceptions.RequestException as e:
		print(f"API 요청 중 오류 발생: {e}")
		return None

# 재난 메시지 저장
def post_disaster_messages(base_url, disaster_messages, headers):
	url = f"{base_url}/v1/datacollector/disasters"

	disasters_payload = []
	for message in disaster_messages:
		locations = message.rcptn_rgn_nm.split(',')
		for location in locations:
			location = location.replace("전체", "")
			disaster_data = {
				"generatedAt": datetime.strptime(message.crt_dt, "%Y/%m/%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%S"),
				"messageId": message.sn,
				"message": message.msg_cn,
				"locationStr": preprocessing_address(location),
				"disasterType": message.dst_se_nm
			}
			disasters_payload.append(disaster_data)

	payload = {"disasters": disasters_payload}

	try:
		response = requests.post(url, headers=headers, json=payload)
		response.raise_for_status()
		data = response.json()
		if data["code"] == 1000:
			print(f"재난 정보 저장 성공: {response.status_code}")
		else:
			print(f"API 오류: {data['message']}")
	except requests.exceptions.RequestException as e:
		print(f"재난 정보 저장 중 오류 발생: {e}")

def get_disaster_messages(url, service_key, page_no, num_of_rows, crt_dt):
	params = {
		"serviceKey": service_key,
		"pageNo": page_no,
		"numOfRows": num_of_rows,
		"crtDt": crt_dt  # YYYYMMDD 형식으로 전달
	}

	try:
		response = requests.get(url, params=params)
		response.raise_for_status()
		data = response.json()

		# 응답 코드 확인
		if data["header"]["resultCode"] == "00" and data["body"]:
			messages = []
			for message in data["body"]:
				# DisasterMessage 객체 생성 후 리스트에 추가
				disaster_msg = DisasterMessage(
					msg_cn=message['MSG_CN'],
					rcptn_rgn_nm=message['RCPTN_RGN_NM'],
					crt_dt=message['CRT_DT'],
					dst_se_nm=message['DST_SE_NM'],
					sn=message['SN']
				)
				messages.append(disaster_msg)
			return messages
		elif data["body"]:
			print(f"API 오류: {data['header']['errorMsg']}")
			return []
		else:
			return []
	except requests.exceptions.RequestException as e:
		print(f"API 요청 중 오류 발생: {e}")
		return []

def lambda_handler(event, context):
	# 메인 로직
	secret = getSecret()

	# secret에서 가져온 값 사용
	headers = {
	"Authorization": f"Bearer {secret['ADMIN_ACCESS_TOKEN']}",
	"Content-Type": "application/json"
	}

	api_base_url = secret["API_SERVER_BASE_URL"]
	disaster_message_api_url = secret["DISASTER_MESSAGE_API_URL"]
	service_key = secret["DISASTER_MESSAGE_API_SERVICE_KEY"]
	page_no = 1
	num_of_rows = 200

	# 오늘 날짜를 YYYYMMDD 형식으로 설정
	crt_dt = (datetime.now()).strftime("%Y%m%d")

	# 최근 재난 메시지 ID 조회
	latest_message_id = get_latest_message_id(api_base_url, headers)

	if latest_message_id is not None:
		print(f"최근 재난 메시지 ID: {latest_message_id}")

		# 모든 재난 메시지 조회
		disaster_messages = get_disaster_messages(disaster_message_api_url, service_key, page_no, num_of_rows, crt_dt)

		# 최신 재난 메시지 ID보다 작은 SN을 가진 메시지 필터링
		new_disaster_messages = [msg for msg in disaster_messages if msg.sn > latest_message_id]

		# SN 값을 기준으로 내림차순 정렬
		new_disaster_messages_sorted = sorted(new_disaster_messages, key=lambda x: x.sn, reverse=True)

		# 필터링된 재난 메시지들 저장 요청
		if new_disaster_messages_sorted:
			post_disaster_messages(api_base_url, new_disaster_messages_sorted, headers=headers)
		else:
			print("저장할 새로운 재난 메시지가 없습니다.")
	else:
		print("최신 재난 메시지를 조회할 수 없습니다.")

#lambda_handler(None, None)