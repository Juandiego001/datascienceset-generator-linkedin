from config import API_KEY
import openai
import pandas as pd
import datetime
import os
import time

openai.api_key = API_KEY
logs = ""

def set_logs():
    count_logs = len(os.listdir("./LOGS/salary_chatgpt/"))
    count_logs += 1
    log_file = open("./LOGS/salary_chatgpt/%s.txt" % count_logs, "w+", encoding='utf-8')
    return log_file

def add_log(message):
    global logs
    logs += str(message) + "\n"

def main():
    # Para establecer los logs.
    log_file = set_logs()

    try:
        # Impresión de mensaje: "Tiempo de inicio:\n"
        time_executed = datetime.datetime.now()
        add_log("Tiempo de inicio: %s\n" % time_executed)
        print("Tiempo de inicio: %s\n" % time_executed)

        # Se verifica si el archivo de salaries.csv existe.
        salaries_csv = False
        file_exists = False
        if os.path.exists("./CSVS/salary_chatgpt/salaries.csv"):
            file_exists = True
            salaries_csv = pd.read_csv("./CSVS/salary_chatgpt/salaries.csv", index_col=0)

        # Se toman todos los job_id que han sido guardados en el .csv de salaries.csv 
        # para no repetir procesos.
        salaries_job_id = []
        if file_exists:
            salaries_job_id = salaries_csv["job_id"].to_numpy(str)    

        jobs_csv = pd.read_csv("./CSVS/linkedin/jobs.csv")
        job_descriptions = jobs_csv["descripcion"]

        salaries_temp_df = pd.DataFrame({"job_id": [], "salario": []})
        jobs_id = []
        salaries = []
        
        # El límite de peticiones que se harán será de 100 cada vez que se ejecute la función.
        max_count = 0
        for key, description in job_descriptions.items():
            if max_count == 100:
                break

            job_id = str(jobs_csv.loc[key]["job_id"])

            if not(job_id in salaries_job_id):
                new_salary = get_job_salary(description)
                salaries.append(new_salary)
                jobs_id.append(job_id)

                add_log("Job_Id: %s\nSalario encontrado: %s\n" % (job_id, new_salary))
                print("Job_Id: %s\nSalario encontrado: %s\n" % (job_id, new_salary))

                # ChatGPT rate: 3/min -> Cada 20 segundos se debe hacer la petición
                time.sleep(20)
            
            max_count += 1

        salaries_temp_df["job_id"] = jobs_id
        salaries_temp_df["salario"] = salaries

        # Si el archivo de salaries.csv anteriormente existió, es porque 
        # ya se lleva un registro de los salarios. Ante lo cual,
        # se concatena nada más el dataframe temporal.
        # De lo contrario, se guarda directamente el salaries_temp_df.
        # Si el tamaño del dataframe de salaries_temp_df es 0, no es necesario hacer el proceso de concatenación.
        if file_exists and len(salaries_temp_df) > 0:
            # Impresión de mensaje: "Se inserta el dataframe temporal en el dataframe anteriormente leido.\n"
            add_log("Se inserta el dataframe temporal en el dataframe anteriormente leido.\n")
            print("Se inserta el dataframe temporal en el dataframe anteriormente leido.\n")        
            salaries_full = salaries_csv.append(salaries_temp_df)
            salaries_full = salaries_full.reset_index(drop=True)

            print(salaries_full)

            salaries_full.to_csv("./CSVS/salary_chatgpt/salaries.csv")
        elif len(salaries_temp_df) > 0:
            # Impresión de mensaje: "Se guarda el daframe temporal directamente en el csv y se establece como nuevo salaries.csv.\n"
            add_log("Se guarda el daframe temporal directamente en el csv y se establece como nuevo salaries.csv.\n")
            print("Se guarda el daframe temporal directamente en el csv y se establece como nuevo salaries.csv.\n")
            salaries_temp_df.to_csv("./CSVS/salary_chatgpt/salaries.csv")

        # Impresión de mensaje: "Guardando datos de salarios en csv y finalizando proceso."
        add_log("Guardando datos de salarios en csv y finalizando proceso.\n")
        print("Guardando datos de salarios en csv y finalizando proceso.\n")

        # Impresión de mensaje: "Cerrando el archivo de salaries.csv ..."
        add_log("Cerrando el archivo de salaries.csv ...\n")
        print("Cerrando el archivo salaries.csv ...\n")

        # Impresión de mensaje: "Tiempo de ejecución total (segundos):\n"
        time_end = datetime.datetime.now()
        total_seconds = (time_end - time_executed).total_seconds()
        add_log("Tiempo de ejecución total (segundos):%s\n" % total_seconds)
        print("Tiempo de ejecución total (segundos):%s\n" % total_seconds)

        # Cerrando log_file.
        log_file.write(logs)
        log_file.close()
    except Exception as err:
        # Para el manejo de errores
        add_log("Ocurrió un error:\n")
        add_log(str(err) + "\n")
        add_log(str(type(err)) + "\n")
        log_file.write(logs)
        log_file.close()
        
        print("Ocurrió un error:\n")
        print(str(err))
        print(type(err))


# Obtener salario a partir de la descripción de los trabajos con ayuda de ChatGPT.
# Para verificar la cantidad de tokens: https://colab.research.google.com/drive/1UaqURXn8SrKpeu1V3wFSS_q8p9UjBX3U#scrollTo=DUEaFZ-T4mev
def get_job_salary(job_description=""):
    the_message = "Calculate the average salary payment per month in USDT for the following job offer:\n%s\n\nPlease return only the number." % job_description
    add_log("Mensaje para enviar a ChatGPT:\n%s\n" % the_message)
    print("Mensaje para enviar a ChatGPT:\n%s\n" % the_message)

    add_log("Enviando mensaje...\n")
    print("Enviando mensaje...\n")
    
    completion = openai.ChatCompletion.create(
      model="gpt-3.5-turbo",
      messages=[
        {
            "role": "user",
            "content": the_message
        }
    ]
    )

    salary_found = completion.choices[0].message.content
    add_log("Respuesta de ChatGPT:\n%s\n" % salary_found)
    print("Respuesta de ChatGPT:\n%s\n" % salary_found)
    
    return salary_found

if __name__ == '__main__':
    main()