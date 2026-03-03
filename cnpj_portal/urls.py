from django.contrib import admin
from django.http import HttpResponse
from django.urls import include, path


def docs_view(request):
    html = """
    <!DOCTYPE html>
    <html>
      <head>
        <title>Portal CNPJ - API Docs</title>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">
        <style>body { margin: 0; padding: 0; }</style>
      </head>
      <body>
        <redoc spec-url='/api/openapi.yaml'></redoc>
        <script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"> </script>
      </body>
    </html>
    """
    return HttpResponse(html)


def openapi_yaml_view(request):
    import os

    from django.conf import settings

    yaml_path = os.path.join(settings.BASE_DIR, "docs", "openapi.yaml")
    with open(yaml_path, encoding="utf-8") as f:
        return HttpResponse(f.read(), content_type="application/yaml")


urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/docs/", docs_view, name="api-docs"),
    path("api/openapi.yaml", openapi_yaml_view, name="openapi-yaml"),
    path("", include("cnpj.urls")),
]
