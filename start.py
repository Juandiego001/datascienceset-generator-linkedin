from config import email, password
from linkedin_api import Linkedin

api = Linkedin(email, password)
profile = api.get_profile('juan-josé-sandoval-delgado-078084264')
print(profile)
