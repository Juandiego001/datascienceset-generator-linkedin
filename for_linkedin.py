from config import email, password, API_KEY
from linkedin_api import Linkedin
from requests_html import HTMLSession
import pandas as pd
import datetime
import openai
import os

# Variables globales
session = HTMLSession()
api = Linkedin(email, password)
openai.api_key = API_KEY
logs = ""

def set_logs():
    count_logs = len(os.listdir("./LOGS/linkedin/"))
    count_logs += 1
    log_file = open("./LOGS/linkedin/%s.txt" % count_logs, "w+", encoding='utf-8')
    return log_file

def add_log(message):
    global logs
    logs += str(message) + "\n"

def main():
    global session
    global api
    global logs

    # Creación del archivo de jobs_linkedin.csv en la carpeta de linkedin en CSVS.
    jobs_file = open("./CSVS/linkedin/jobs.csv", "w+")

    # Creación del archivo de companys_linkedin.csv en la carpeta de linkedin en CSVS.
    companys_file = open("./CSVS/linkedin/companys.csv", "w+")

    # Para establecer los logs.
    log_file = set_logs()

    try:
        # Impresión de mensaje: "Tiempo de inicio:\n"
        time_executed = datetime.datetime.now()
        add_log("Tiempo de inicio: %s\n" % time_executed)
        print("Tiempo de inicio: %s\n" % time_executed)

        # Impresión de mensaje: "1- Creando dataframe de Jobs...\n"
        add_log("1- Creando dataframe de Jobs...\n")
        print("1- Creando dataframe de Jobs...\n")

        # Creación del Dataframe de jobs.
        jobs_df = pd.DataFrame({"job_id": [], 
                      "company_id": [], 
                      "titulo": [], 
                      "descripcion": [], 
                      "fecha_publicacion": [],
                      "nivel_antiguedad": [],
                      "tipo_empleo": [],
                      "funcion_laboral": [],
                      "sectores": [],
                      "salario": []})
        
        add_log("Dataframe de jobs:\n")
        add_log(jobs_df.to_string() + "\n")

        # Impresión de mensaje: "2- Dataframe y archivo de Jobs creado.\n"
        add_log("2- Dataframe y archivo de Jobs creado.\n")
        print("2- Dataframe y archivo de Jobs creado.\n")

        # Impresión de mensaje: "3- Creando dataframe de Companys...\n"
        add_log("3- Creando dataframe de Companys...\n")
        print("3- Creando dataframe de Companys...\n")

        # Creación del Dataframe de companys.
        companys_df = pd.DataFrame({"company_id": [], 
                      "nombre": [], 
                      "url": []})
        
        add_log("Dataframe de companys:\n")
        add_log(companys_df.to_string() + "\n")

        # Impresión de mensaje: "4- Dataframe y archivo de Companys creado.\n"
        add_log("4- Dataframe y archivo de Companys creado.\n")
        print("4- Dataframe y archivo de Companys creado.\n")

        # Impresión de mensaje: "5- Inicio de proceso de búsqueda de trabajos.\n"
        add_log("5- Inicio de proceso de búsqueda de trabajos.\n")
        print("5- Inicio de proceso de búsqueda de trabajos.\n")

        # Se asignan los valores a cada columna (series) del dataframe.
        jobs_df["job_id"],\
            jobs_df["company_id"],\
                jobs_df["titulo"],\
                    jobs_df["descripcion"],\
                        jobs_df["fecha_publicacion"],\
                            jobs_df["nivel_antiguedad"],\
                                jobs_df["tipo_empleo"],\
                                    jobs_df["funcion_laboral"],\
                                        jobs_df["sectores"],\
                                            jobs_df["salario"] = search_jobs()

        add_log("Columna de job_id.\n")
        add_log(jobs_df["job_id"].to_string() + "\n")

        add_log("Columna de company_id.\n")
        add_log(jobs_df["company_id"].to_string() + "\n")

        add_log("Columna de titulo.\n")
        add_log(jobs_df["titulo"].to_string() + "\n")

        add_log("Columna de descripcion.\n")
        add_log(jobs_df["descripcion"].to_string() + "\n")

        add_log("Columna de fecha_publicacion.\n")
        add_log(jobs_df["fecha_publicacion"].to_string() + "\n")

        add_log("Columna de nivel_antiguedad.\n")
        add_log(jobs_df["nivel_antiguedad"].to_string() + "\n")

        add_log("Columna de tipo_empleo.\n")
        add_log(jobs_df["tipo_empleo"].to_string() + "\n")

        add_log("Columna de funcion_laboral.\n")
        add_log(jobs_df["funcion_laboral"].to_string() + "\n")

        add_log("Columna de sectores.\n")
        add_log(jobs_df["sectores"].to_string() + "\n")

        add_log("Columna de salario.\n")
        add_log(jobs_df["salario"].to_string() + "\n")

        # Impresión de mensaje: "6- Guardando datos de trabajo en csv."
        add_log("6- Guardando datos de trabajo en csv.\n")
        print("6- Guardando datos de trabajo en csv.\n")

        # Se guarda el dataframe de jobs en el .csv creado
        jobs_df.to_csv("./CSVS/linkedin/jobs.csv")

        # Impresión de mensaje: "7- Iniciando proceso de extracción de información para compañías..."
        add_log("7- Iniciando proceso de extracción de información para compañías...\n")
        print("7- Iniciando proceso de extracción de información para compañías...\n")

        companys_id = []
        companys_name = []
        companys_url = []
        for index, company_id in jobs_df["company_id"].items():
            add_log("company_id: " + str(company_id) + "\n")
            companys_id.append(company_id)
            company_name, company_url = get_company_info(company_id)

            add_log("company_name: " + str(company_name) + "\n")
            add_log("company_url: " + str(company_url) + "\n")

            companys_name.append(company_name)
            companys_url.append(company_url)

        companys_df["company_id"] = companys_id
        companys_df["nombre"] = companys_name
        companys_df["url"] = companys_url

        add_log("Columna de company_id en company_df.\n")
        add_log(companys_df["company_id"].to_string() + "\n")

        add_log("Columna de nombre en company_df.\n")
        add_log(companys_df["nombre"].to_string() + "\n")

        add_log("Columna de url en company_df.\n")
        add_log(companys_df["url"].to_string() + "\n")

        # Impresión de mensaje: "8- Guardando datos de trabajo en csv."
        add_log("8- Guardando datos de empresas en csv y finalizando proceso.\n")
        print("8- Guardando datos de empresas en csv y finalizando proceso.\n")

        # Se guarda el dataframe de jobs en el .csv creado
        companys_df.to_csv("./CSVS/linkedin/companys.csv")

        # Impresión de mensaje: "9- Cerrando los archivos...\n"
        add_log("9- Cerrando los archivos...\n")
        print("9- Cerrando los archivos...\n")
        jobs_file.close()
        companys_file.close()

        # Impresión de mensaje: "Tiempo de ejecución total (segundos):\n"
        time_end = datetime.datetime.now()
        total_seconds = (time_end - time_executed).total_seconds()
        add_log("Tiempo de ejecución total (segundos):%s\n" % total_seconds)
        print("Tiempo de ejecución total (segundos):%s\n" % total_seconds)
        
        log_file.write(logs)
        log_file.close()
    except Exception as err:
        # En caso de error, se cierran los archivos.
        jobs_file.close()
        companys_file.close()

        # Para el manejo de errores
        add_log("Ocurrió un error:\n")
        add_log(str(err) + "\n")
        add_log(str(type(err)) + "\n")
        log_file.write(logs)
        log_file.close()

        print("Ocurrió un error:\n")
        print(str(err))
        print(type(err))


def search_jobs(keywords="data"):
    global api

    # Impresión de mensaje: "Iniciando búsqueda de trabajos...\n"
    add_log("Iniciando búsqueda de trabajos...\n")
    print("Iniciando búsqueda de trabajos...\n")

    published_time_limit = 60*24*60*60
    location = "Colombia"
    limit_offers = 10000
    searchJobs = api.search_jobs(keywords, listed_at=published_time_limit, location_name=location, distance=None, limit=limit_offers)

    # Impresión de mensaje: "Cantidad de trabajos encontrados:\n"
    add_log("Cantidad de trabajos encontrados: %s\n" % len(searchJobs))
    print("Cantidad de trabajos encontrados: %s\n" % len(searchJobs))

    all_jobs_ids = []
    all_jobs_companys_ids = []
    all_jobs_titles = []
    all_jobs_descriptions = []
    all_jobs_dates = []
    all_jobs_salaries = []

    # Skills
    all_jobs_seniority_level = []
    all_jobs_employment_type = []
    all_jobs_job_function = []
    all_jobs_industries = []

    # Impresión de mensaje: "Ingresando al ciclo para iterar sobre cada búsqueda...\n"
    add_log("Ingresando al ciclo para iterar sobre cada búsqueda...\n")
    print("Ingresando al ciclo para iterar sobre cada búsqueda...\n")

    for job_found in searchJobs:
        # Llaves en el diccionario de job_found
        job_found_keys = job_found.keys()

        # Verificamos que exista la llave 'dashEntityUrn'
        job_id = None
        if "dashEntityUrn" in job_found_keys:
            job_id = job_found["dashEntityUrn"].split(":")[-1]

        company_id = None
        # Verificamos que existe la llave 'companyDetails'
        if "companyDetails" in job_found_keys:
            job_found_company_keys = job_found["companyDetails"].keys()

            # Verificamos que existe la llave 'company' en 'companyDetails'
            if "company" in job_found_company_keys:
                company_id = job_found["companyDetails"]["company"].split(":")[-1]

        # Verificamos que exista la llave 'title'
        job_title = None
        if "title" in job_found_keys:
            job_title = job_found["title"]

        job_description = get_job_description(api, job_id)

        # Se produjo un error.
        # job_salary = get_job_salary(job_description)
        job_salary = 0

        job_date = datetime.date.fromtimestamp(int(job_found["listedAt"])/1000)

        add_log("job_id: \n")
        add_log(job_id)
        add_log("company_id: \n")
        add_log(company_id)
        add_log("job_title: \n")
        add_log(job_title)
        add_log("job_description: \n")
        add_log(job_description)
        add_log("job_date: \n")
        add_log(job_date.strftime("%d/%m/%Y %H:%M:%S"))

        all_jobs_ids.append(job_id)
        all_jobs_companys_ids.append(company_id)
        all_jobs_titles.append(job_title)
        all_jobs_descriptions.append(job_description)
        all_jobs_dates.append(job_date)
        all_jobs_salaries.append(job_salary)

        seniority_level, employment_type, job_function, industries = get_job_skills(job_id)

        add_log("seniority_level: \n")
        add_log(seniority_level)
        add_log("employment_type: \n")
        add_log(employment_type)
        add_log("job_function: \n")
        add_log(job_function)
        add_log("industries: \n")
        add_log(industries)

        all_jobs_seniority_level.append(seniority_level)
        all_jobs_employment_type.append(employment_type)
        all_jobs_job_function.append(job_function)
        all_jobs_industries.append(industries)
    
    # Impresión de mensaje: "Ciclo finalizado. Enviando trabajos al proceso principal...\n"
    add_log("Ciclo finalizado. Enviando trabajos al proceso principal...\n")
    print("Ciclo finalizado. Enviando trabajos al proceso principal...\n")

    return (all_jobs_ids, 
            all_jobs_companys_ids, 
            all_jobs_titles, 
            all_jobs_descriptions, 
            all_jobs_dates,
            all_jobs_seniority_level,
            all_jobs_employment_type,
            all_jobs_job_function,
            all_jobs_industries,
            all_jobs_salaries)

def get_company_info(company_id):
    global api

    company_name, company_url = None, None

    # Si el company_id existe, entonces obtenga los datos de la compañía.
    if company_id:
        the_comapany = None

        # Se maneja el error encontrado con id número 15333945
        try:
            the_comapany = api.get_company(company_id)
        except:
            add_log("Evidenciado error con company_id: %s" % company_id)

        # Verificamos que si existe un valor en the_company
        if the_comapany:
            company_name = the_comapany["name"]
            company_url = the_comapany["url"]

    return (company_name, company_url)

def get_job_description(api, job_id):
    # Si no hemos obtenido un job_id retornamos None
    if not(job_id):
        return None
    else:
        description_searched = api.get_job(job_id)
        description_searched_keys = description_searched.keys()
        the_description = None

        # Verificamos que exista la llave "description"
        if "description" in description_searched_keys:
            description_found_keys = description_searched["description"].keys()

            # Verificamos que exista la llave "text" en "description"
            if "text" in description_found_keys:
                the_description = description_searched["description"]["text"]

        return the_description

# URL de ejemplo: 'https://www.linkedin.com/jobs/view/3555997418'
def get_job_skills(job_id):
    global session
    r = session.get("https://www.linkedin.com/jobs/view/%s" % job_id)

    # Para observar si los títulos corresponden al seniority level, employment type y así.
    h3Elements = r.html.find(".description__job-criteria-subheader")
    spanElements = r.html.find(".description__job-criteria-text")

    seniority_level = None
    employment_type = None
    job_function = None
    industries = None

    count = 0
    for h3 in h3Elements:
        if h3.text == 'Seniority level':
            seniority_level = spanElements[count].text
        elif h3.text == 'Employment type':
            employment_type = spanElements[count].text
        elif h3.text == 'Job function':
            job_function = spanElements[count].text
        elif h3.text == 'Industries':
            industries = spanElements[count].text
    
        count += 1

    return (seniority_level, employment_type, job_function, industries)

# # Obtener salario a partir de la descripción de los trabajos con ayuda de ChatGPT.
# # Para verificar la cantidad de tokens: https://colab.research.google.com/drive/1UaqURXn8SrKpeu1V3wFSS_q8p9UjBX3U#scrollTo=DUEaFZ-T4mev
# def get_job_salary(job_description=""):

#     the_message = "Calculate the average salary payment per month for the following job offer:\n%s\n\nPlease return only the number." % job_description
#     print("Mensaje para enviar a ChatGPT:\n%s\n" % the_message)
#     print("Enviando mensaje...\n")
    
#     completion = openai.ChatCompletion.create(
#       model="gpt-3.5-turbo",
#       messages=[
#         {
#             "role": "user",
#             "content": the_message
#         }
#     ]
#     )

#     salary_found = completion.choices[0].message.content
#     print("Respuesta de ChatGPT:\n%s\n" % salary_found)
    
#     return salary_found

# Cuando se esté probando y se ejecute directamente el for_linkedin.python.
if __name__ == '__main__':
    main()