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

## 목표

- Avro 스키마를 사용한 JSON 데이터 직렬화 성능 측정
- Go 서버와 Unreal Engine 클라이언트 간 효율적인 데이터 통신 구현
- 다양한 데이터 크기와 구조에 대한 성능 비교 분석

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