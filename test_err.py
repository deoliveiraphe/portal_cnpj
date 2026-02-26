import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'cnpj_portal.settings')
django.setup()

from django.test import Client
c = Client(HTTP_HOST='localhost')
try:
    r = c.get('/busca/?cnpj=Banco+do+brasil')
    print('STATUS:', r.status_code)
except Exception as e:
    import traceback
    traceback.print_exc()
