from datetime import datetime
from django.shortcuts import render
from django.http import HttpRequest
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
import datetime
import math
from django.db import connection
import psycopg2
import json

possible_columns = ["id","br_court_name","kind_name","cin","registration_date","corporate_body_name","br_section","br_insertion","text","street","postal_code","city"]
required_post_columns = ["br_court_name","kind_name","cin","registration_date","corporate_body_name","br_section","br_insertion","text","street","postal_code","city"]
pos_columns_companies = ["cin", "name", "br_section", "address_line", "last_update", "or_podanie_issues_count", "znizenie_imania_issues_count", "likvidator_issues_count", "konkurz_vyrovnanie_issues_count", "konkurz_restrukturalizacia_actors_count"]

#zadanie_1
def uptime(request):
    with connection.cursor() as cur:
        cur.execute("SELECT json_object_agg('uptime', date_trunc('second',current_timestamp - pg_postmaster_start_time()));")
        time = cur.fetchone()
        tmp = {
            "pgsql" : time[0]
            }
    return HttpResponse(json.dumps(tmp), content_type = 'application/json')

#zadanie_2
@csrf_exempt
def check_method(request):
    if (request.method == 'GET'):
        return get_submissions(request)
    elif (request.method == 'POST'):
        return post_submissions(request)

def get_submissions(request):
    with connection.cursor() as cur:
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

        if (query_arg):
            query_stmt += f" WHERE (corporate_body_name ILIKE '%%{query_arg}%%' OR cin::varchar ILIKE '%%{query_arg}%%' OR city ILIKE '%%{query_arg}%%')"
            if (reg_date_gte or reg_date_lte):
                query_stmt+= ' AND'
                if (reg_date_gte and reg_date_lte):
                    query_stmt+= f" (registration_date >= '{reg_date_gte}' AND '{reg_date_lte}' >= registration_date)"
                elif (reg_date_gte):
                    query_stmt+= f" (registration_date >= '{reg_date_gte}')"
                else:
                    query_stmt+= f" (registration_date <= '{reg_date_lte}')"
        else:
            if (reg_date_gte or reg_date_lte):
                query_stmt+= ' WHERE'
                if (reg_date_gte and reg_date_lte):
                    query_stmt+= f" (registration_date >= '{reg_date_gte}' AND '{reg_date_lte}' >= registration_date)"
                elif (reg_date_gte):
                    query_stmt+= f" (registration_date >= '{reg_date_gte}')"
                else:
                    query_stmt+= f" (registration_date <= '{reg_date_lte}')"

        order_by_arg = request.GET.get('order_by', 'id')
        #validacia order_by_arg -> default = id
        if (order_by_arg not in possible_columns):
            order_by_arg = 'id'
        order_type_arg = request.GET.get('order_type', 'asc')
        #validacia order_type_arg -> default = asc
        if (order_type_arg.lower() != 'asc' and order_type_arg.lower() != 'desc'):
            order_type_arg = 'asc'   

        offset = (int(page_arg) * int(per_page_arg)) - int(per_page_arg)
        cur.execute("SELECT json_build_object('id',id,'br_court_name',br_court_name,'kind_name',kind_name,'cin',cin,'registration_date',registration_date,'corporate_body_name',corporate_body_name,'br_section',br_section,'br_insertion',br_insertion,'text',text,'street',street,'postal_code',postal_code,'city',city) " + 
                    'FROM ov.or_podanie_issues'+ query_stmt+ ' ORDER BY '+order_by_arg+ ' '+order_type_arg.upper()+ ' OFFSET ' + str(offset) + ' LIMIT '+ per_page_arg)
        selected_items = cur.fetchall()
        
        cur.execute('SELECT COUNT(id) FROM ov.or_podanie_issues'+ query_stmt)
        total = cur.fetchone()
        pages = math.ceil(total[0]/int(per_page_arg))
        tmp = []
        for item in selected_items:
            tmp.append(item[0])
        get_fin = {
            "items": tmp,
            "metadata": {
                "page": int(page_arg),
                "per_page": int(per_page_arg),
                "pages": pages,
                "total": total[0]}
            }
    return HttpResponse(json.dumps(get_fin), content_type = 'application/json', status = 200)

def create_error(field, reason):
    dictErr = {
        "field": field,
        "reasons": reason
        }
    return dictErr

def post_submissions(request):
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
    else:
        with connection.cursor() as cur:
            actYear = int(now.strftime("%Y"))
            published_at = now.today()
            created_at = now.today()
            updated_at = now.today()
            cur.execute(f"SELECT number FROM ov.bulletin_issues WHERE year = {actYear} ORDER BY number DESC LIMIT 1")
            number_list = cur.fetchone()
            if (number_list):
                number = number_list[0] + 1
            else:
                number = 1
            cur.execute(f"INSERT INTO ov.bulletin_issues (year,number,published_at,created_at, updated_at) VALUES ({actYear},{number},'{published_at}','{created_at}','{updated_at}') RETURNING id")
            bull_iss_id = int(cur.fetchone()[0])
            cur.execute(f"INSERT INTO ov.raw_issues (bulletin_issue_id, file_name,content,created_at,updated_at) VALUES ({bull_iss_id},'-','-','{created_at}','{updated_at}') RETURNING id")
            raw_iss_id = int(cur.fetchone()[0])
            addressLine = body['street'] + ', '+ body['postal_code'] + ' ' + body['city']
            cur.execute('INSERT INTO ov.or_podanie_issues (bulletin_issue_id,raw_issue_id,br_mark,br_court_code,br_court_name,kind_code,kind_name,cin,registration_date,corporate_body_name,br_section,br_insertion,text,created_at,updated_at,address_line,street,postal_code,city)'+
                        f" VALUES ({bull_iss_id},{raw_iss_id},'-','-','{body['br_court_name']}','-','{body['kind_name']}',{body['cin']},'{body['registration_date']}','{body['corporate_body_name']}','{body['br_section']}','{body['br_insertion']}','{body['text']}','{created_at}','{updated_at}','{addressLine}','{body['street']}','{body['postal_code']}','{body['city']}')"+
                        " RETURNING json_build_object('id',id,'br_court_name',br_court_name,'kind_name',kind_name,'cin',cin,'registration_date',registration_date,'corporate_body_name',corporate_body_name,'br_section',br_section,'br_insertion',br_insertion,'text',text,'street',street,'postal_code',postal_code,'city',city)")
            insertedSubm = cur.fetchone()[0]
            post_fin = {
                "response": insertedSubm,}
            connection.commit()
            return HttpResponse(json.dumps(post_fin), content_type = 'application/json', status = 201)

def delete_submissions(request, id):
    with connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(id) FROM ov.or_podanie_issues WHERE id = {id}")
        idExists = cur.fetchone()[0]
        if (idExists == 0):
            return HttpResponse(status = 404)
        else:
            cur.execute(f"DELETE FROM ov.or_podanie_issues WHERE id = {id} returning raw_issue_id")
            rawId = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(id) FROM ov.or_podanie_issues WHERE raw_issue_id = {rawId}")
            moreRaws = cur.fetchone()[0]
            if (moreRaws == 0):
                cur.execute(f"DELETE FROM ov.raw_issues WHERE id = {rawId} returning bulletin_issue_id")
                bullID = cur.fetchone()[0]
                cur.execute(f"SELECT COUNT(id) FROM ov.raw_issues WHERE bulletin_issue_id = {bullID}")
                moreBulls = cur.fetchone()[0]
                if (moreBulls == 0):
                    cur.execute(f"DELETE FROM ov.bulletin_issues WHERE id = {bullID}")
                    connection.commit()
                    return HttpResponse(status = 204)
                else:
                    connection.commit()
                    return HttpResponse(status = 204)
            else:
                connection.commit()
                return HttpResponse(status = 204)
        
def get_companies(request):
    with connection.cursor() as cur:
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
        last_update_lte = request.GET.get('last_update_lte', 0)
        last_update_gte = request.GET.get('last_update_gte', 0)

        if (query_arg):
            query_stmt += f" WHERE (comp.name ILIKE '%%{query_arg}%%' OR comp.address_line ILIKE '%%{query_arg}%%')"
            if (last_update_gte or last_update_lte):
                query_stmt+= ' AND'
                if (last_update_gte and last_update_lte):
                    query_stmt+= f" (comp.last_update >= '{last_update_gte}' AND '{last_update_lte}' >= comp.last_update)"
                elif (last_update_gte):
                    query_stmt+= f" (comp.last_update >= '{last_update_gte}')"
                else:
                    query_stmt+= f" (comp.last_update <= '{last_update_lte}')"
        else:
            if (last_update_gte or last_update_lte):
                query_stmt+= ' WHERE'
                if (last_update_gte and last_update_lte):
                    query_stmt+= f" (comp.last_update >= '{last_update_gte}' AND '{last_update_lte}' >= comp.last_update)"
                elif (last_update_gte):
                    query_stmt+= f" (comp.last_update >= '{last_update_gte}')"
                else:
                    query_stmt+= f" (comp.last_update <= '{last_update_lte}')"

        order_by_arg = request.GET.get('order_by', 'comp.cin')
        #validacia order_by_arg -> default = cin
        if (order_by_arg not in pos_columns_companies):
            order_by_arg = 'comp.cin'
        order_type_arg = request.GET.get('order_type', 'asc')
        #validacia order_type_arg -> default = asc
        if (order_type_arg.lower() != 'asc' and order_type_arg.lower() != 'desc'):
            order_type_arg = 'asc'   

        offset = (int(page_arg) * int(per_page_arg)) - int(per_page_arg)

        cur.execute("""SELECT json_build_object('cin', comp.cin,
                        'name', comp.name, 
                        'br_section', comp.br_section, 
                        'address_line', comp.address_line, 
                        'last_update', comp.last_update, 
                        'or_podanie_issues_count', pod_cin.or_podanie_issues_count, 
                        'znizenie_imania_issues_count', zniz_cin.znizenie_imania_issues_count, 
                        'likvidator_issues_count', likv_cin.likvidator_issues_count, 
                        'konkurz_vyrovnanie_issues_count', konk_cin.konkurz_vyrovnanie_issues_count, 
                        'konkurz_restrukturalizacia_actors_count', konkR_cin.konkurz_restrukturalizacia_actors_count)
                    FROM ov.companies as comp
                    LEFT JOIN (SELECT COUNT(ov.or_podanie_issues.cin) AS or_podanie_issues_count, ov.or_podanie_issues.cin FROM ov.or_podanie_issues  GROUP BY ov.or_podanie_issues.cin) 
                        AS pod_cin ON comp.cin = pod_cin.cin
                    LEFT JOIN (SELECT COUNT(ov.znizenie_imania_issues.cin) AS znizenie_imania_issues_count, ov.znizenie_imania_issues.cin FROM ov.znizenie_imania_issues GROUP BY ov.znizenie_imania_issues.cin) 
                        AS zniz_cin ON comp.cin = zniz_cin.cin
                    LEFT JOIN (SELECT COUNT(ov.likvidator_issues.cin) AS likvidator_issues_count, ov.likvidator_issues.cin FROM ov.likvidator_issues GROUP BY ov.likvidator_issues.cin) 
                        AS likv_cin ON comp.cin = likv_cin.cin
                    LEFT JOIN (SELECT COUNT(ov.konkurz_vyrovnanie_issues.cin) AS konkurz_vyrovnanie_issues_count, ov.konkurz_vyrovnanie_issues.cin FROM ov.konkurz_vyrovnanie_issues GROUP BY ov.konkurz_vyrovnanie_issues.cin) 
                        AS konk_cin ON comp.cin = konk_cin.cin
                    LEFT JOIN (SELECT COUNT(ov.konkurz_restrukturalizacia_actors.cin) AS konkurz_restrukturalizacia_actors_count, ov.konkurz_restrukturalizacia_actors.cin FROM ov.konkurz_restrukturalizacia_actors GROUP BY ov.konkurz_restrukturalizacia_actors.cin) 
                        AS konkR_cin ON comp.cin = konkR_cin.cin """ + 
                    query_stmt+ ' ORDER BY '+order_by_arg+ ' '+order_type_arg.upper()+ ' OFFSET ' + str(offset) + ' LIMIT '+ per_page_arg)
        selected_items = cur.fetchall()
        
        cur.execute('SELECT COUNT(cin) FROM ov.companies AS comp'+ query_stmt)
        total = cur.fetchone()
        pages = math.ceil(total[0]/int(per_page_arg))
        tmp = []
        for item in selected_items:
            tmp.append(item[0])
        get_fin = {
            "items": tmp,
            "metadata": {
                "page": int(page_arg),
                "per_page": int(per_page_arg),
                "pages": pages,
                "total": total[0]}
            }
    return HttpResponse(json.dumps(get_fin), content_type = 'application/json', status = 200)

        
            
            
            