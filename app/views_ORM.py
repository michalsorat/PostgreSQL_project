from django.shortcuts import render
from django.http import HttpRequest
from django.http import HttpResponse
from app.models import *
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Q
import psycopg2
import json
import math
from datetime import datetime

possible_columns = ["id","br_court_name","kind_name","cin","registration_date","corporate_body_name","br_section","br_insertion","text","street","postal_code","city"]

@csrf_exempt
def check_method_ORM(request, id = None):
    if (request.method == 'GET'):
        if (id == None):
            return get_submissions_ORM(request)
        else:
            return get_submissions_by_id(request, id)
    elif (request.method == 'POST'):
        return post_submissions_ORM(request)
    elif (request.method == 'DELETE'):
        return delete_submissions_ORM(request, id)
    elif (request.method == 'PUT'):
        return put_submission_ORM(request, id)

def get_item(item):
    return {"id": item.id,\
            "br_court_name": item.br_court_name,\
            "kind_name": item.kind_name,\
            "cin": item.cin,\
            "registration_date": item.registration_date,\
            "corporate_body_name": item.corporate_body_name,\
            "br_section": item.br_section,\
            "br_insertion": item.br_insertion,\
            "text": item.text,\
            "street": item.street,\
            "postal_code": item.postal_code,\
            "city": item.city }

def get_submissions_ORM(request):
    query_stmt = ''
    page_arg = request.GET.get('page', '1')
    #validacia page_arg -> ak nepozostava len z cislic page_arg -> default = 1
    if not(page_arg.isdigit() and int(page_arg) > 0):
        page_arg = '1'
    per_page_arg = request.GET.get('per_page', '10')
    #validacia per_page_arg -> ak nepozostava len z cislic per_page_arg -> default = 10
    if not(per_page_arg.isdigit() and int(per_page_arg) > 0):
        per_page_arg = '10'
    
    query_arg = request.GET.get('query', 0)
    reg_date_lte = request.GET.get('registration_date_lte', 0)
    reg_date_gte = request.GET.get('registration_date_gte', 0)

    if (reg_date_lte != 0):
        reg_date_lte = datetime.fromisoformat(reg_date_lte)
    if (reg_date_gte != 0):
        reg_date_gte = datetime.fromisoformat(reg_date_gte)

    query_stmt = Q()
    if (query_arg):
        query_stmt = Q(corporate_body_name__iexact = query_arg) & Q(cin__iexact = query_arg) & Q(city__iexact = query_arg)
        if (reg_date_gte or reg_date_lte):
            if (reg_date_gte and reg_date_lte):
                query_stmt &= Q(registration_date__gte = reg_date_gte) & Q(registration_date__lte = reg_date_lte)
            elif (reg_date_gte):
                query_stmt &= Q(registration_date__gte = reg_date_gte)
            else:
                query_stmt &= Q(registration_date__lte = reg_date_lte)
    else:
        if (reg_date_gte or reg_date_lte):
            if (reg_date_gte and reg_date_lte):
                query_stmt &= Q(registration_date__gte = reg_date_gte) & Q(registration_date__lte = reg_date_lte)
            elif (reg_date_gte):
                query_stmt &= Q(registration_date__gte = reg_date_gte)
            else:
                query_stmt &= Q(registration_date__lte = reg_date_lte)
    
    order_by_arg = request.GET.get('order_by', 'id')
    #validacia order_by_arg -> default = id
    if (order_by_arg not in possible_columns):
        order_by_arg = 'id'
    order_type_arg = request.GET.get('order_type', 'asc')
    #validacia order_type_arg -> default = asc
    if (order_type_arg.lower() != 'asc' and order_type_arg.lower() != 'desc'):
            order_type_arg = 'asc'
    if (order_type_arg.lower() == 'desc'):
        order_by_arg = '-' + order_by_arg 

    offset = (int(page_arg) * int(per_page_arg)) - int(per_page_arg)
    qs = OrPodanieIssues.objects.filter(query_stmt).order_by(order_by_arg)[offset:offset+int(per_page_arg)]
    count = OrPodanieIssues.objects.filter(query_stmt).count()
    pages = math.ceil(count/int(per_page_arg))
    get_fin = {
        "items": [],
        "metadata": {
            "page": int(page_arg),
            "per_page": int(per_page_arg),
            "pages": pages,
            "total": count
            }
        }
    for item in qs:
        get_fin["items"].append(get_item(item))
    return HttpResponse(json.dumps(get_fin, default=str), content_type = 'application/json', status = 200)

def post_submissions_ORM(request):
    listOfErrors = []
    errors = {"errors": listOfErrors}
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    for item in required_post_columns:
        if not (item in body):
            listOfErrors.append(create_error(item, "required"))
        elif (item == 'cin'):
            if not (isinstance(body[item], int)):
                listOfErrors.append(create_error(item, "not_number"))
        elif (item == 'registration_date'):
            try:
                datetime.datetime.fromisoformat(body[item])
            except:
                listOfErrors.append(create_error(item, "required"))
            else:
                dt = datetime.datetime.strptime(body[item], '%Y-%m-%d')
                now = datetime.datetime.now()
                if (dt.year != now.year):
                    listOfErrors.append(create_error(item, "invalid range"))
    if (len(listOfErrors)):
        return HttpResponse(json.dumps(errors), content_type = 'application/json', status = 422)
    #else:
        #actYear = int(now.strftime("%Y"))
        #published_at = now.today()
        #created_at = now.today()
        #updated_at = now.today()
        #number_list = cur.fetchone()
        #if (number_list):
        #    number = number_list[0] + 1
        #else:
        #    number = 1
        #bull_iss_id = int(cur.fetchone()[0])
        #raw_iss_id = int(cur.fetchone()[0])
        #addressLine = body['street'] + ', '+ body['postal_code'] + ' ' + body['city']
        #post_fin = {
        #    "response": insertedSubm,}
        #connection.commit()
        #return HttpResponse(json.dumps(post_fin), content_type = 'application/json', status = 201)


def delete_submissions_ORM(request, id):
    exists = OrPodanieIssues.objects.filter(id=id).exists()
    if (exists == 0):
        return HttpResponse(status = 404)
    else:
        pod_issues = OrPodanieIssues.objects.get(pk=id)
        if (OrPodanieIssues.objects.filter(id = pod_issues.raw_issue_id).count() == 1):
            RawIssues.objects.filter(id = pod_issues.raw_issue_id).delete()
            if (RawIssues.objects.filter(id = pod_issues.bulletin_issue_id).count() == 0):
                BulletinIssues.objects.filter(id = pod_issues.bulletin_issue_id).delete()
        pod_issues.delete()
        return HttpResponse(status = 204)

def get_submissions_by_id(request, id):
    exists = OrPodanieIssues.objects.filter(id=id).exists()
    if (exists == 0):
        return HttpResponse(status = 404)
    else:
        item = OrPodanieIssues.objects.get(pk=id)
        result = {
            "response": get_item(item)
            }
        return HttpResponse(json.dumps(result, default=str), content_type = 'application/json', status = 200)

def create_error(field, reason):
    dictErr = {
        "field": field,
        "reasons": reason
        }
    return dictErr

def put_submission_ORM(request, id):
    listOfErrors = []
    errors = {"errors": listOfErrors}
    body_unicode = request.body.decode('utf-8')
    body = json.loads(body_unicode)
    exists = OrPodanieIssues.objects.filter(id=id).exists()
    if (exists == 0):
        return HttpResponse(status = 404)
    else:
        object = OrPodanieIssues.objects.get(id=id)
        for item in body.keys():
            if (item == 'cin'):
                if not (isinstance(body[item], int)):
                    listOfErrors.append(create_error(item, "not_number"))
            elif (item == 'street'):
                if not (isinstance(body[item], str)):
                    listOfErrors.append(create_error(item, "not_string"))
            elif (item == 'registration_date'):
                try:
                    datetime.datetime.fromisoformat(body[item])
                except:
                    listOfErrors.append(create_error(item, "invalid_range"))
                else:
                    dt = datetime.datetime.strptime(body[item], '%Y-%m-%d')
                    now = datetime.datetime.now()
                    if (dt.year != now.year):
                        listOfErrors.append(create_error(item, "invalid range"))
        if (len(listOfErrors)):
            return HttpResponse(json.dumps(errors), content_type = 'application/json', status = 422)
        else:
            for item in body.keys():
                setattr(object, item, body[item])
                object.save
        result = {
            "response": get_item(object)
            }
        return HttpResponse(json.dumps(result, default=str), content_type = 'application/json', status = 200)