# PySpark Student Data Capstone Project

## Project Overview
This project uses Apache PySpark to perform a comprehensive data analysis of student records. The analysis addresses 20 distinct business questions, focusing on academic performance, behavioral factors, and the outcomes of internship participation.

## Files
- `students_core.csv`: Student demographic and school information.
- `academic_performance.csv`: Academic records, including GPA, attendance, and discipline incidents.
- `internship_data.csv`: Records of student internships and expected status.
- `capstone_analysis.py`: The main PySpark script that loads, joins, transforms the data (e.g., Q12 Classification), and outputs the answers to all 20 questions.
- `README.md`: This documentation file.

## How to Run the Analysis
Since all files are in the root directory:

1.  Ensure you have a working Apache Spark environment setup.
2.  Save all five files above into a single directory.
3.  Open your command prompt or terminal and navigate to that directory.
4.  Execute the script using `spark-submit`:
    ```bash
    spark-submit capstone_analysis.py
    ```

## Key Analysis Examples
- **Student Segmentation (Q12):** Implemented logic to classify students into At-Risk, High Potential, and Average groups.
- **Correlation Analysis (Q5, Q19):** Calculated average academic performance by gender and tracked high-risk students (low attendance) through their internship completion status.