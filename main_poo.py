import stat
import mysql.connector
import pandas as pd
import sys 
import matplotlib.pyplot as plt

class DataBaseConection:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None


    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            print("Connection successful")
        except mysql.connector.Error as e:
            print(e)
            sys.exit(1)

    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection closed")

    def get_cursor(self):
        return self.connection.cursor()
    
class EmployeePerformanceDatabase:
    def __init__(self, db_connection):
        self.db_connection = db_connection 

    def create_table(self):
        cursor = self.db_connection.get_cursor()
        cursor.execute("DROP TABLE IF EXISTS EmployeePermormance")
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS EmployeePerformance(
                id INT AUTO_INCREMENT PRIMARY KEY,
                employee_id INT,
                department VARCHAR(255),
                performance_score DECIMAL(5,2),
                years_with_company INT,
                salary DECIMAL(10,2)
            )
        """)
        self.db_connection.connection.commit()
        print("Tabla creada correctamente")


    def import_csv_to_db(self, csv_file):
        cursor = self.db_connection.get_cursor()
        df = pd.read_csv(csv_file)
        print("Columnas en el CSV:", df.columns)
        for i, row in df.itemsrrows():
            values = tuple(row[col] for col in ['employee_id', 'department', 'performance_score', 'years_with_company', 'salary'])
            sql_insert = """INSERT INTO EmployeePerformance (employee_id, department, performance_score, years_with_company, salary)
                            VALUES (%s, %s, %s, %s, %s)"""
            cursor.execute("""
                INSERT INTO EmployeePerformance (employee_id, department, performance_score, years_with_company, salary)
                VALUES (%s, %s, %s, %s, %s)
            """, values)
            self.db_connection.conecction.commit()
            print("Datos importados correctamente")


    def query_all(self):
        return pd.read_sql_query("SELECT * FROM EmployeePerformance", self.db_connection.connection)
    

class EmployeeStatistics:
    def __init__(self, data_frame):
        self.df = data_frame
        self.departaments = self.dv['department'].unique()


    def calculate_satistics(self):
        statistics = {}
        for department in self.departaments:
            df_department = self.df[self.df['department'] == department]


            statistics[department] = {
                'performance_score_media': df_department['performance_score'].mean(),
                'performance_score_mediana': df_department['performance_score'].median(),
                'performance_score_desviacion': df_department['performance_score'].std(),
                'salary_media': df_department['salary'].mean(),
                'salary_mediana': df_department['salary'].median(),
                'salary_desviacion': df_department['salary'].std(),
                'num_empleados': df_department.shape[0],
                'correlacion_years_performance': df_department['years_with_company'].corr(df_department['performance_score']),
                'correlacion_salary_performance': df_department['salary'].corr(df_department['performance_score']),
            }
        return statistics

    def display_statistics(self, statistics):
        for department, stats in statistics.items():
            print(f"Departamento: {department}")
            print(f"Media del performance_score: {stats['performance_score_media']}")
            print(f"Mediana del performance_score: {stats['performance_score_mediana']}")
            print(f"Desviación estándar del performance_score: {stats['performance_score_desviacion']}")
            print(f"Media del salary: {stats['salary_media']}")
            print(f"Mediana del salary: {stats['salary_mediana']}")
            print(f"Desviación estándar del salary: {stats['salary_desviacion']}")
            print(f"Número total de empleados: {stats['num_empleados']}")
            print(f"Correlación entre years_with_company y performance_score: {stats['correlacion_years_performance']}")
            print(f"Correlación entre salary y performance_score: {stats['correlacion_salary_performance']}")
            print()


class PlotGenerator:
    def __init__(self, data_frame):
        self.df = data_frame

    def plot_histogram(self):
        for department in self.df['department'].unique():
            df_department = self.df[self.df['department'] == department]
            plt.hist(df_department['performance_score'], bins=10, alpha=0.5, label=department)
        plt.xlabel('Performance Score')
        plt.ylabel('Frecuencia')
        plt.title('Histograma del Performance Score por Departamento')
        plt.legend()
        plt.show()

    def plot_scatter(self, x_col, y_col, x_label, y_label, title):
        plt.scatter(self.df[x_col], self.df[y_col])
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.title(title)
        plt.show()



db_connection = DataBaseConection(host="localhost", user="root", password="", database="companydata")
db_connection.connect()

employee_db = EmployeePerformanceDatabase(db_connection)
employee_db.create_table()
employee_db.import_csv('./MOCK_DATA.csv')

df = employee_db.query_all()

statistics = EmployeeStatistics(df)
stats = statistics.calculate_statistics()
statistics.display_statistics(stats)

plotter = PlotGenerator(df)
plotter.plot_histogram()
plotter.plot_scatter('years_with_company', 'performance_score', 'Años con la empresa', 'Performance Score', 'Gráfico de dispersión de years_with_company vs. performance_score')
plotter.plot_scatter('salary', 'performance_score', 'Salario', 'Performance Score', 'Gráfico de dispersión de salary vs. performance_score')

db_connection.close()