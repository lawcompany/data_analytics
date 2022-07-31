# data_analytics
데이터분석팀 전용 리파지토리

# 폴더 구조 설명
adhoc : adhoc 분석 소스코드(sql,python) 저장소
batch : batch 작업(dags) 소스코드 저장소
  - dags : composer의 airflow dags와 연동되어 있음(CI/CD 적용됨)

# 원격저장소로 commit되지 않게 하고 싶은 파일이나 확장자(ex: *.log)가 있다면
.gitignore 파일에 추가 하세요
