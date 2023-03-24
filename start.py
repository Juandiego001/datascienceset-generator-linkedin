from config import email, password
from linkedin_api import Linkedin

api = Linkedin(email, password)
searchJobs = api.search_jobs("data science ciencia de datos", limit=5)
print(searchJobs)