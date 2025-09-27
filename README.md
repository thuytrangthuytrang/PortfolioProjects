
📊 TheLook E-commerce SQL Analytics
📌 Giới thiệu

TheLook là một trang web thương mại điện tử quần áo giả tưởng do nhóm Looker phát triển. Bộ dữ liệu chứa thông tin về khách hàng, sản phẩm, đơn hàng, hậu cần, sự kiện web và chiến dịch tiếp thị kỹ thuật số. Bộ dữ liệu này được công khai trên Google BigQuery. Dự án này sử dụng SQL để phân tích dữ liệu nhằm khai thác insight kinh doanh từ hoạt động thương mại điện tử.

🎯 Mục tiêu dự án
- Thống kê và phân tích hành vi mua sắm của khách hàng.
- Xác định xu hướng doanh thu, giá trị đơn hàng, sản phẩm nổi bật.
- Phân tích đặc điểm khách hàng theo độ tuổi và giới tính.
- Thực hiện cohort analysis để theo dõi tỷ lệ khách hàng quay lại.

🛠️ Công cụ sử dụng
- Google BigQuery: Truy vấn dữ liệu.
- SQL: Xử lý và phân tích dữ liệu.




📊 Sales Data Cleaning & RFM Analysis
📌 Giới thiệu

Bộ dữ liệu mô phỏng dữ liệu bán hàng của một công ty phân phối mô hình xe hơi. Dự án này thực hiện làm sạch dữ liệu (Data Cleaning) và phân tích RFM (Recency – Frequency – Monetary) trên bộ dữ liệu Sales Dataset.

🎯 Mục tiêu dự án
- Chuẩn hóa và xử lý dữ liệu bán hàng nhằm đảm bảo tính chính xác và toàn vẹn.
- Phân tích hành vi khách hàng để tìm ra nhóm khách hàng tiềm năng và các sản phẩm/doanh thu nổi bật.

1️⃣ Phần 1: Data Cleaning
- Chuẩn hóa kiểu dữ liệu.
- Kiểm tra giá trị NULL, loại bỏ outlier.
- Chuẩn bị dữ liệu cho phân tích RFM.

2️⃣ Phần 2: RFM Analysis
- Phân tích doanh thu theo nhiều chiều (thời gian, sản phẩm, khu vực).
- Xếp hạng khách hàng theo Recency (R), Frequency (F), Monetary (M) để tìm ra khách hàng tốt nhất.

🧩 Mô hình RFM
- Recency (R): Khoảng thời gian từ lần mua gần nhất đến hiện tại.
- Frequency (F): Số lần mua hàng.
- Monetary (M): Tổng tiền chi tiêu.
- Điểm RFM = Ghép các phân vị (R_score + F_score + M_score).
- Kết hợp với bảng segment_score để phân nhóm khách hàng: Best Customer, Loyal, Potential, Churn Risk, v.v.

🚀 Kết quả & Insight tiêu biểu
- Tháng bán tốt nhất: Xác định tháng cao điểm để tối ưu marketing.
- Top ProductLine theo quốc gia: Gợi ý danh mục sản phẩm chủ lực.
- Best Customer: Lọc ra nhóm khách hàng chi tiêu cao, tần suất lớn, gần đây nhất → ưu tiên chăm sóc & giữ chân.
