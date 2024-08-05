import mysql.connector
import pandas as pd
import sys
import matplotlib.pyplot as plt

# Funcion para conctar a la base de datos
def connect_db():
    try:
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="companydata",
            port="3208"  
        )
    except mysql.connector.Error as e:
        print("No se pudo conectar:", e)
        sys.exit(1)
    print("Conexión correcta")
    return db

# Funcion para importar datos de un CSV a la base de datos
def import_csv_to_db(csv_file, db):
    cursor = db.cursor()
    df = pd.read_csv(csv_file)
    print("Columnas en el CSV:", df.columns)
    for i, row in df.iterrows():
        values = tuple(row[col] for col in ['employee_id', 'department', 'performance_score', 'years_with_company', 'salary'])
        sql_insert = """INSERT INTO EmployeePerformance (employee_id, department, performance_score, years_with_company, salary)
                        VALUES (%s, %s, %s, %s, %s)"""
        cursor.execute(sql_insert, values)
    db.commit()
    print("Datos importados correctamente")
db = connect_db()
import_csv_to_db('./MOCK_DATA.csv', db)


# Consulta a la base de datos
df = pd.read_sql_query("SELECT * FROM EmployeePerformance", db)

# Estadísticas
departamentos = df['department'].unique()

# Diccionario para guardar las estadísticas de cada departamento
estadísticas = {}


for departamento in departamentos:

    df_departamento = df[df['department'] == departamento]

    # Media, mediana y desviación estándar del performance_score

    performance_score_media = df_departamento['performance_score'].mean()

    performance_score_mediana = df_departamento['performance_score'].median()

    performance_score_desviacion = df_departamento['performance_score'].std()

    # Media, mediana y desviación estándar del salario
    salary_media = df_departamento['salary'].mean()

    salary_mediana = df_departamento['salary'].median()

    salary_desviacion = df_departamento['salary'].std()

    # Número total de empleados en el departamento
    num_empleados = df_departamento.shape[0]

    # Correlación entre years_with_company y performance_score
    correlacion_years_performance = df_departamento['years_with_company'].corr(df_departamento['performance_score'])
    
    # Correlación entre salary y performance_score
    correlacion_salary_performance = df_departamento['salary'].corr(df_departamento['performance_score'])
    

    estadísticas[departamento] = {

        'performance_score_media': performance_score_media,

        'performance_score_mediana': performance_score_mediana,

        'performance_score_desviacion': performance_score_desviacion,

        'salary_media': salary_media,

        'salary_mediana': salary_mediana,

        'salary_desviacion': salary_desviacion,

        'num_empleados': num_empleados,

        'correlacion_years_performance': correlacion_years_performance,

        'correlacion_salary_performance': correlacion_salary_performance

    }



for departamento, estadística in estadísticas.items():

    print(f"Departamento: {departamento}")

    print(f"Media del performance_score: {estadística['performance_score_media']}")

    print(f"Mediana del performance_score: {estadística['performance_score_mediana']}")

    print(f"Desviación estándar del performance_score: {estadística['performance_score_desviacion']}")

    print(f"Media del salary: {estadística['salary_media']}")

    print(f"Mediana del salary: {estadística['salary_mediana']}")

    print(f"Desviación estándar del salary: {estadística['salary_desviacion']}")

    print(f"Número total de empleados: {estadística['num_empleados']}")

    print(f"Correlación entre years_with_company y performance_score: {estadística['correlacion_years_performance']}")

    print(f"Correlación entre salary y performance_score: {estadística['correlacion_salary_performance']}")

    print()
    

# Gráficos
for departamento in departamentos:
    df_departamento = df[df['department'] == departamento]
    plt.hist(df_departamento['performance_score'], bins=10, alpha=0.5, label=departamento)
plt.xlabel('Performance Score')
plt.ylabel('Frecuencia')
plt.title('Histograma del Performance Score por Departamento')
plt.legend()
plt.show()


plt.scatter(df['years_with_company'], df['performance_score'])
plt.xlabel('Años con la empresa')
plt.ylabel('Performance Score')
plt.title('Gráfico de dispersión de years_with_company vs. performance_score')
plt.show()


plt.scatter(df['salary'], df['performance_score'])
plt.xlabel('Salario')
plt.ylabel('Performance Score')
plt.title('Gráfico de dispersión de salary vs. performance_score')
plt.show()