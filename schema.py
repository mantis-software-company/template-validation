
from marshmallow import fields,validates, validate,Schema

class Schemas(Schema):
                

    FullName = fields.String(required=True)
    phone =  fields.String(validate=validate.Regexp( r"^(05(\d{9}))$") )
    Email = fields.Email(required=True) 

