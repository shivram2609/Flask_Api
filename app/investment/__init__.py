from flask import Blueprint

blueprint = Blueprint(
    'investment_blueprint',
    __name__,
    url_prefix='/investment',
    template_folder='templates',
    static_folder='static'
)
