"""
Template tags auxiliares do portal CNPJ.

Inclui:
- dict_get: acessa dicionários em templates
- url_replace: modifica parâmetros GET na URL preservando os demais
"""
from django import template

register = template.Library()


@register.filter
def dict_get(d, key):
    """Acessa dicionário por chave em template: {{ meu_dict|dict_get:chave }}"""
    if d is None:
        return None
    return d.get(key)


@register.simple_tag(takes_context=True)
def url_replace(context, **kwargs):
    """
    Substitui ou adiciona parâmetros GET na URL atual, preservando os demais.
    Uso: href="?{% url_replace page=3 %}"
    """
    request = context.get("request")
    if request:
        params = request.GET.copy()
    else:
        params = {}
    for key, value in kwargs.items():
        params[key] = value
    return params.urlencode()
