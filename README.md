# Omnichannel Retail ETL Pipeline

Pipeline แบบ Idempotent และ Incremental ที่แปลงข้อมูลคำสั่งซื้อดิบ (`customers`, `products`,
`orders_batch_1..3`) จาก `Python_Data_Pipeline_Lab_Dataset.xlsx` ให้กลายเป็น Star Schema ใน
SQLite (`retail_dw.db`) พร้อมรายงานคุณภาพข้อมูล

## วิธีติดตั้ง

ต้องมี Python 3.10+ และแพ็กเกจ `pandas`, `openpyxl`

```bash
pip install pandas openpyxl
```

## วิธีรัน

วางไฟล์ `source_dataset.xlsx` (สำเนาของ dataset ต้นฉบับ — **ไม่แก้ไขไฟล์ต้นฉบับ**) ไว้โฟลเดอร์
เดียวกับ `pipeline.py` แล้วรัน:

```bash
python pipeline.py
```

สคริปต์จะสร้าง/เขียนทับไฟล์ต่อไปนี้ในโฟลเดอร์เดียวกัน:

| ไฟล์ | รายละเอียด |
|---|---|
| `retail_dw.db` | SQLite Star Schema หลังโหลดครบ 3 batch |
| `quarantine.csv` | ระเบียนที่ไม่ผ่านการตรวจสอบ พร้อม `reason_code` |
| `pipeline_run_log.csv` | ประวัติการรันและ KPI แต่ละรอบ |

Demo ใน `main()` รันตามลำดับ 4 รอบตามที่โจทย์กำหนด: `batch_1` → `batch_1` (รันซ้ำ) →
`batch_2` → `batch_3` เพื่อพิสูจน์ทั้ง Idempotency และ Incremental Loading

จะเรียกใช้แบบ custom เองก็ได้ผ่าน `PipelineConfig` + `run_pipeline()`:

```python
from pipeline import PipelineConfig, run_pipeline

config = PipelineConfig(
    input_path="source_dataset.xlsx",
    output_db="retail_dw.db",
    batches=[1, 2, 3],
    error_mode="quarantine",  # หรือ "fail_fast"
)
run_pipeline(config)
```

## โครงสร้าง Star Schema

```
                dim_date
                    |
dim_customer --- fact_sales --- dim_product
```

| ตาราง | คีย์ | คอลัมน์หลัก |
|---|---|---|
| `dim_customer` | `customer_key` (PK, autoincrement) | `customer_id` (unique), `customer_name`, `province`, `segment` |
| `dim_product` | `product_key` (PK, autoincrement) | `product_id` (unique), `product_name`, `category` |
| `dim_date` | `date_key` (PK, `YYYYMMDD`) | `full_date`, `day`, `month`, `quarter`, `year` |
| `fact_sales` | `order_id` (PK) | `date_key`, `customer_key`, `product_key` (FK), `quantity`, `unit_price`, `discount_pct`, `gross_amount`, `net_amount`, `payment_method`, `sales_channel`, `source_batch`, `updated_at` |

**Grain ของ fact_sales**: หนึ่งแถว = หนึ่งรายการขายสินค้าที่ผ่านการตรวจสอบแล้ว ต่อ `order_id`
(รับประกันด้วย `PRIMARY KEY(order_id)`)

ตารางสนับสนุน: `quarantine` (ระเบียนที่ถูกปฏิเสธ + เหตุผล) และ `pipeline_run_log`
(watermark/ประวัติการรันแต่ละ batch)

## กลไก Idempotency และ Incremental Loading

- โหลดข้อมูลลง `fact_sales` ด้วย `INSERT ... ON CONFLICT(order_id) DO UPDATE`
- ก่อนเขียน จะเทียบ `updated_at` ใหม่กับค่าที่มีอยู่แล้วในตาราง — โหลด/อัปเดตเฉพาะกรณีที่
  `updated_at` ใหม่กว่าเท่านั้น หากเท่ากันหรือเก่ากว่าจะข้าม (skip)
- ผลลัพธ์: รัน batch เดิมซ้ำกี่ครั้งก็ไม่ทำให้จำนวนแถวใน `fact_sales` เพิ่มขึ้น
  (พิสูจน์ได้จาก RUN 1 vs RUN 2 ใน `pipeline_run_log.csv` — `rows_loaded` เปลี่ยนจาก 376 เป็น 0)
- ระเบียนที่มี `order_id` เดิมแต่ `updated_at` ใหม่กว่า (เช่น order ที่มาซ้ำใน batch ถัดไป)
  จะถูกอัปเดตทับแบบ upsert โดยไม่เพิ่มจำนวนแถว

## สูตร KPI ใน pipeline_run_log

```
rows_read = rows_valid + rows_rejected      (นับก่อนการ deduplicate)
rows_rejected = ข้อมูลที่ผิดกฎ (data quality) + rows_duplicated (สำเนาซ้ำที่ถูกแทนที่)
rows_loaded = แถวที่ insert หรือ update จริงใน fact_sales รอบนั้น (อาจน้อยกว่า rows_valid
              เมื่อรันซ้ำและข้อมูลไม่มีการเปลี่ยนแปลง)
```

## Data Quality Rules ที่ตรวจสอบ (Task 2)

| กฎ | reason_code เมื่อไม่ผ่าน |
|---|---|
| `customer_id` ต้องมีและมีอยู่จริงใน `dim_customer` | `missing_customer_id`, `customer_not_found` |
| `product_id` ต้องมีและมีอยู่จริงใน `dim_product` | `missing_product_id`, `product_not_found` |
| สินค้าต้อง active | `inactive_product` |
| `quantity` เป็นจำนวนเต็ม 1-20 | `invalid_quantity` |
| `unit_price` เป็นตัวเลขและ > 0 (รองรับ prefix `THB`) | `invalid_unit_price` |
| `discount_pct` อยู่ระหว่าง 0-100 | `invalid_discount_pct` |
| `order_datetime` / `updated_at` แปลงเป็นวันที่ได้ | `invalid_order_datetime`, `invalid_updated_at` |
| `payment_method` normalize ได้ (case-insensitive) | `invalid_payment_method` |
| `sales_channel` normalize ได้ (`E-Commerce` → `Online`) | `invalid_sales_channel` |
| `order_id` ซ้ำภายใน batch เดียวกัน — เก็บเฉพาะ `updated_at` ล่าสุด | `superseded_duplicate` |

แถวที่ผิดหลายกฎพร้อมกันจะมี `reason_code` หลายค่าคั่นด้วย `;`

## Reflection: เหตุใด Availability จึงมักสำคัญกว่า Strictness ใน Production Pipeline

ใน Production ข้อมูลจริงมักไม่สมบูรณ์เสมอ หากออกแบบ Pipeline ให้หยุดทำงานทันทีที่เจอแถวผิดพลาด
เพียงแถวเดียว (Strictness สูงสุด) ผลกระทบคือข้อมูลที่ถูกต้องอีกหลายพันแถวจะไม่ถูกโหลดไปด้วย
ทำให้ฝ่ายวิเคราะห์ไม่มีข้อมูลใช้งานเลย แม้ปัญหาจะเกิดกับข้อมูลเพียงส่วนน้อยก็ตาม การแยกข้อมูลเสีย
ออกเป็น quarantine พร้อม reason_code ทำให้ระบบยังคง "พร้อมใช้งาน" (Available) และส่งมอบคุณค่า
ทางธุรกิจต่อเนื่อง ในขณะที่ทีมวิศวกรรมสามารถไปแก้ไขข้อมูลที่มีปัญหาเป็นรอบถัดไปได้โดยไม่กระทบ
รอบที่โหลดสำเร็จแล้ว (Fail-safe ไม่ใช่ Fail-stop)

นอกจากนี้ Availability ยังสอดคล้องกับหลัก Idempotency และ Incremental Loading ของ Pipeline นี้
กล่าวคือหากรอบใดรอบหนึ่งล้มเหลวบางส่วน ระบบยังสามารถรันซ้ำได้อย่างปลอดภัยโดยไม่สร้างข้อมูลซ้ำ
หรือทำลายข้อมูลที่โหลดไปแล้ว ต่างจาก Strictness ที่มักออกแบบมาแบบ all-or-nothing ซึ่งเปราะบาง
กว่าเมื่อข้อมูลต้นทางมีขนาดใหญ่และมาจากหลายช่องทางพร้อมกัน

แน่นอนว่า Strictness ยังจำเป็นในบางบริบท เช่น ธุรกรรมทางการเงินที่ผิดพลาดแล้วแก้ไขยาก แต่สำหรับ
Data Warehouse เพื่อการวิเคราะห์อย่างในเคสนี้ การเลือก "ส่งมอบข้อมูลที่ถูกต้องบางส่วนได้ต่อเนื่อง"
มีคุณค่าทางธุรกิจสูงกว่า "หยุดทั้งระบบเพื่อรอความสมบูรณ์แบบ 100%" อย่างชัดเจน
