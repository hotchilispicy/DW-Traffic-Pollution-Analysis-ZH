from benzin_neu_LF import lambda_handler1
from benzin_LF import lambda_handler2
from rosengarten_street_LF import lambda_handler3
from stampfenbach_LF import lambda_handler4
from zurich_main_LF import lambda_handler5
from diesel_LF import lambda_handler6
from diesel_neu_LF import lambda_handler7
from hybrid_LF import lambda_handler8
from hybrid_neu_LF import lambda_handler9
from elektro_LF import lambda_handler10
from elektro_neu_LF import lambda_handler11
from other_LF import lambda_handler12
from other_new_LF import  lambda_handler13

def lambda_handler(event, context):
    # Dispatch based on an event key
    function_to_invoke = event.get('function')
    if function_to_invoke == 'handler1':
        return lambda_handler1(event, context)
    elif function_to_invoke == 'handler2':
        return lambda_handler2(event, context)
    elif function_to_invoke == 'handler3':
        return lambda_handler3(event, context)
    elif function_to_invoke == 'handler4':
        return lambda_handler4(event, context)
    elif function_to_invoke == 'handler5':
        return lambda_handler5(event, context)
    elif function_to_invoke == 'handler6':
        return lambda_handler6(event, context)
    elif function_to_invoke == 'handler7':
        return lambda_handler7(event, context)
    elif function_to_invoke == 'handler8':
        return lambda_handler8(event, context)
    elif function_to_invoke == 'handler9':
        return lambda_handler9(event, context)
    elif function_to_invoke == 'handler10':
        return lambda_handler10(event, context)
    elif function_to_invoke == 'handler11':
        return lambda_handler11(event, context)
    elif function_to_invoke == 'handler12':
        return lambda_handler12(event, context)
    elif function_to_invoke == 'handler13':
        return lambda_handler13(event, context)
    else:
        return {
            'statusCode': 400,
            'body': 'Invalid function specified'
        }