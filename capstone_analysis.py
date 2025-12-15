from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, avg, regexp_replace, lower
from pyspark.sql.types import DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CapstoneStudentAnalysis_All_20_Questions") \
    .getOrCreate()

# ADJUSTED PATH: Since files are in the same directory, use "./"
data_path = "./"

# 1. Load DataFrames
try:
    df_core = spark.read.csv(data_path + "students_core.csv", header=True, inferSchema=True)
    df_academic = spark.read.csv(data_path + "academic_performance.csv", header=True, inferSchema=True)
    df_internship = spark.read.csv(data_path + "internship_data.csv", header=True, inferSchema=True)
    
    # Ensure numerical columns are correctly cast for calculations
    df_academic = df_academic.withColumn("GPA", col("GPA").cast(DoubleType())) \
                             .withColumn("AttendanceRate", col("AttendanceRate").cast(DoubleType()))
    
except Exception as e:
    print(f"Error loading data. Ensure the three CSV files are in the same directory as this script: {e}")
    spark.stop()
    exit()

# 2. Create Comprehensive DataFrame (Joining)
df_all = df_core.join(df_academic, "StudentID", "inner") \
               .join(df_internship, "StudentID", "left")

# --- CORE PREPARATION: STUDENT CLASSIFICATION (Q12 LOGIC) ---
# Student Classification Rules
df_classified = df_all.withColumn(
    "StudentClassification",
    when((col("AttendanceRate") < 0.8) | (col("DisciplineIncidents") > 2), lit("At-Risk"))
    .when((col("AttendanceRate") >= 0.8) & (col("DisciplineIncidents") <= 2) & (lower(col("ExtraCurAct")) != lit("none")), lit("High Potential"))
    .otherwise(lit("Average"))
)
df_classified.cache()

# =========================================================
#                   SOLVING QUESTIONS 1 - 20
# (The rest of the script remains the same as before, performing the analysis)
# =========================================================

print("\n" + "="*50)
print("             PYSPARK CAPSTONE ANALYSIS RESULTS")
print("="*50 + "\n")

# Q1: Display the students from 'Tampa' city.
print("1. Students from 'Tampa' city:")
df_classified.filter(col("Location") == "Tampa").select("StudentID", "Location", "GPA").show(5)

# Q2: Display the students who have an extracurricular activity.
print("2. Students with an Extracurricular Activity:")
df_classified.filter(lower(col("ExtraCurAct")) != lit("none")).select("StudentID", "ExtraCurAct", "GPA").show(5)

# Q3: Display the students who are in grade 12 and GPA greater than 3.5.
print("3. Students in Grade 12 with GPA > 3.5:")
df_classified.filter((col("GradeLevel") == 12) & (col("GPA") > 3.5)).select("StudentID", "GradeLevel", "GPA").show(5)

# Q4: Display the student details who were born in Sept or Oct of 1998, 1999, 2000.
print("4. Students born in Sept or Oct of 1998, 1999, 2000:")
df_q4 = df_classified.withColumn("BirthMonthYear", regexp_replace(col("DOB"), r"^\d{2}-", ""))
df_q4.filter(
    (col("BirthMonthYear").isin("09-1998", "10-1998", "09-1999", "10-1999", "09-2000", "10-2000"))
).select("StudentID", "DOB", "GPA").show(5)

# Q5: Display the average academic performance by gender.
print("5. Average Academic Performance (GPA, AttendanceRate) by Gender:")
df_classified.groupBy("Gender").agg(
    avg("GPA").alias("Avg_GPA"),
    avg("AttendanceRate").alias("Avg_AttendanceRate")
).show(5)

# Q6: Display the student population distribution by grade level.
print("6. Student Population Distribution by Grade Level:")
df_classified.groupBy("GradeLevel").agg(
    count("*").alias("Student_Count")
).orderBy("GradeLevel").show(5)

# Q7: Display the total number of students dropped out.
print("7. Total Number of Students Dropped Out:")
dropped_count = df_classified.filter(col("DropoutStatus") == "Dropped").count()
print(f"Total students dropped out: {dropped_count}")

# Q8: Display the students with the highest academic achievement (Top 3 GPA).
print("8. Top 3 Students by GPA:")
df_classified.orderBy(col("GPA").desc()).limit(3).select("StudentID", "GPA", "Location").show(5)

# Q9: Display the students' GPA based on the type of school (Public, Private, Charter).
print("9. Average GPA by School Type:")
df_classified.groupBy("Public_Private_Charter").agg(
    avg("GPA").alias("Avg_GPA")
).show(5)

# Q10: Display the list of students who have a GPA less than 2.0.
print("10. List of Students with GPA < 2.0 (At-Risk Academics):")
df_classified.filter(col("GPA") < 2.0).select("StudentID", "GPA", "AttendanceRate").show(5)

# Q11: Display the percentage of students who graduated based on socio-economic status.
print("11. Graduation Rate by Socio-Economic Status (Grade 12 as Proxy):")
df_q11 = df_classified.groupBy("SocioEconomicStatus").agg(
    count(when(col("GradeLevel") == 12, True)).alias("Graduated_Count"),
    count("*").alias("Total_Count")
)
df_q11.withColumn(
    "Graduation_Rate_Percent",
    (col("Graduated_Count") / col("Total_Count") * 100).cast("int")
).select("SocioEconomicStatus", "Graduation_Rate_Percent").show(5)

# Q12: Segment the students into At-Risk, High Potential, and Average. 
print("12. Student Segmentation (At-Risk, High Potential, Average) - Count:")
df_classified.groupBy("StudentClassification").count().orderBy(col("count").desc()).show(5)
# 

# Q13: Count the students who haven't done internships.
print("13. Count of Students Who Haven't Done Internships:")
no_internship_count = df_classified.filter(col("InternshipDone") == "No").count()
print(f"Total students without internships: {no_internship_count}")

# Q14: Display the number of students who completed internships in each company.
print("14. Internships Completed by Company:")
df_classified.filter(col("InternshipDone") == "Yes").groupBy("InternshipCompany").count().orderBy(col("count").desc()).show(5)

# Q15: Display the number of students who completed internships based on location.
print("15. Internships Completed by Location:")
df_classified.filter(col("InternshipDone") == "Yes").groupBy("InternshipLocation").count().orderBy(col("count").desc()).show(5)

# Q16: Display the minimum number of students of each gender who completed internships.
print("16. Internship Completion Count by Gender (and Minimum):")
df_q16 = df_classified.filter(col("InternshipDone") == "Yes").groupBy("Gender").count()
df_q16.show(5)
print("Minimum Internship completions by gender:")
df_q16.orderBy(col("count").asc()).limit(1).show(5)

# Q17: Display the number of students who completed internships in each role.
print("17. Internships Completed by Role:")
df_classified.filter(col("InternshipDone") == "Yes").groupBy("InternshipRole").count().orderBy(col("count").desc()).show(5)

# Q18: Display the list of internships done based on Socio-Economic Status.
print("18. Internships by Socio-Economic Status and Role:")
df_classified.filter(col("InternshipDone") == "Yes").groupBy("SocioEconomicStatus", "InternshipRole").count().orderBy("SocioEconomicStatus", col("count").desc()).show(10)

# Q19: Display the list of students with low attendance who completed internships.
print("19. Low Attendance Students (< 0.8) Who Completed Internships:")
df_classified.filter((col("AttendanceRate") < 0.8) & (col("InternshipDone") == "Yes")) \
             .select("StudentID", "AttendanceRate", "InternshipRole").show(5)

# Q20: Display the number of students whose Expected Status is At-Risk and completed internships.
print("20. At-Risk Students (Expected Status) Who Completed Internships:")
at_risk_intern_count = df_classified.filter((col("ExpectedStatus") == "At Risk") & (col("InternshipDone") == "Yes")).count()
print(f"Total At-Risk (ExpectedStatus) students who completed internships: {at_risk_intern_count}")

# Stop the Spark session
spark.stop()