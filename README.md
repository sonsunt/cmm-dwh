# Portfolio Project 2: Creating an end-to-end crypto price data pipeline
สร้าง Data pipeline จากข้อมูลราคาคริปโตเคอเรนซี่ของ Coinmarketcap

## ที่มาของโปรเจกต์นี้
เป็นการปรับปรุงโปรเจกต์ Data Pipeline หนึ่งใน Reddit ที่ดึงข้อมูลจาก CoinMarketCap แต่เป็นการเก็บข้อมูลภายใน PostgreSQL
ในโปรเจกต์นี้ จะมีการดึงข้อมูลส่งออกไปยัง Data Lake ที่เป็น Google Cloud Storage (GCS) และโหลดเข้า Google Cloud BigQuery (BQ)

## การทำงาน Data Pipeline
### Data Ingestion
- ดึงข้อมูลมาจาก CoinMarketCap API ผ่าน Python
- นำข้อมูลที่ได้จาก CoinMarketCap ในรูปแบบ JSON แปลงเป็น CSV (Flattening) และบันทึกทั้ง CSV และ JSON เข้าไปใน GCS
- โหลด CSV เข้าไปใน BQ 
### Data Transformation
- สร้าง Fact Table กับ Dimension Table จาก ตารางที่เก็บรักษาข้อมูลไว้
### Automating the Pipeline
- กำหนดให้รัน Pipeline ทุก 1 ชั่วโมง

## เครื่องมือที่ใช้
-  Data Ingestion: Python
-  Data Transformation: dbt
-  Orchestrator: Prefect

##  สถานะ
- ✅ สร้าง Minimum Viable Product
- ⏳ ปรับปรุง Idempotency
- ⏳ ปรับปรุง Unit test 
- ✅ เชื่อมต่อ Task แต่ละตัวเข้ากับ Orchestrator
- ⏳ พัฒนางานอย่างต่อเนื่อง
