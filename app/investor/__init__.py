from flask import Blueprint

blueprint = Blueprint(
    'investor_blueprint',
    __name__,
    url_prefix='/investor',
    template_folder='templates',
    static_folder='static'
)
