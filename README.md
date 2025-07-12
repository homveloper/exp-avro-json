# exp-avro-json

Apache Avro를 통한 JSON 데이터 직렬화 최적화 실험 프로젝트

## 개요

이 프로젝트는 Apache Avro를 사용하여 JSON 데이터의 직렬화/역직렬화 성능을 최적화하는 실험적 테스트를 위한 저장소입니다.

## 기술 스택

- **데이터 포맷**: Apache Avro
- **전송 포맷**: JSON
- **서버**: Go (Golang)
- **클라이언트**: Unreal Engine

## 프로젝트 구조

```
exp-avro-json/
├── server/         # Go 서버 구현
├── client/         # Unreal Engine 클라이언트 구현
├── .gitignore
└── README.md
```

## 실험 목표

이 실험의 핵심 목표는 다음과 같습니다:

1. **로그 저장 최적화**
   - 기존 JSON과 Avro+JSON 포맷의 압축률 비교
   - 압축된 데이터의 가독성 확인
   - 실제 로그 데이터를 사용한 성능 측정

2. **실시간 데이터 전송**
   - 서버(Go)와 클라이언트(Unreal Engine) 간 Avro JSON 포맷으로 데이터 전송
   - 네트워크 대역폭 사용량 측정
   - 직렬화/역직렬화 성능 분석

## 시작하기

### 서버 설정

```bash
cd server
# Go 모듈 초기화 및 의존성 설치
go mod init github.com/yourusername/exp-avro-json/server
go get github.com/linkedin/goavro/v2
```

### 클라이언트 설정

1. Unreal Engine 프로젝트 생성
2. 필요한 플러그인 설정
3. Avro 관련 모듈 통합

## 라이선스

이 프로젝트는 실험 목적으로 작성되었습니다.