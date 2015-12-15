import falcon
import json
import datetime
import hashlib
import itsdangerous
import mongoengine
from mongoengine import connect

connect("gdelt")
s = itsdangerous.Signer("ee09f5d40551658fe6a3c52f3a9ede9769604fce1986a3af0a8a05694f32")


class GdeltQueries(mongoengine.Document):
    """Models an individual Guestbook entry with content and date."""
    date = mongoengine.DateTimeField(default=datetime.datetime.now())
    args = mongoengine.StringField(required=True)
    slug = mongoengine.StringField(required=True)
    cached = mongoengine.BooleanField(required=True, default=False)
    hits = mongoengine.IntField(default=1)


def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj


def generate_hash(args, sha1=False):
    res = ''
    for key in sorted(args):
        res += str(date_handler(args[key])) + '_'

    return hashlib.sha1(res).hexdigest() if sha1 else res


class DataResource:
    def on_get(self, req, resp):
        default_args = {
            'int': {
                'min_mentions': 50,
                'min_sources': 1,
                'min_sum': 1,
                'time_step': 7
            },
            'date': {
                'date_from': datetime.date.today() - datetime.timedelta(3 * 365),
                'date_to': datetime.date.today()
            },
            'str': {
                'search': ''
            }
        }

        args = {}

        for key in default_args['int']:
            val = req.get_param_as_int(key)
            if val and val > 0:
                args[key] = val
            else:
                args[key] = default_args['int'][key]

        for key in default_args['date']:
            val = req.get_param_as_date(key)
            if val:
                args[key] = val
            else:
                args[key] = default_args['date'][key]

        for key in default_args['str']:
            val = req.get_param(key)
            if val:
                args[key] = val
            else:
                args[key] = default_args['str'][key]

        print(args)

        gdelt_query = GdeltQueries.objects(slug=generate_hash(args))

        resp.status = falcon.HTTP_202
        resp.body = json.dumps({
            'status': 202
        })

        if not gdelt_query.count():
            ds = GdeltQueries(args=json.dumps(args, default=date_handler), slug=generate_hash(args))
            ds.save()
        else:
            gdelt_query = gdelt_query[0]
            gdelt_query.hits += 1
            gdelt_query.save()

            if gdelt_query.cached:
                resp.status = falcon.HTTP_200
                resp.body = json.dumps({
                    'status': 200,
                    'dataUrl': 'https://storage.googleapis.com/fi-edu-cdn/gdelt-results/json/%s.json' % gdelt_query.slug
                })


class WorkResource:
    def on_get(self, req, resp):
        gdelt_query = GdeltQueries.objects(cached=False).first()

        if gdelt_query:
            resp.body = json.loads(gdelt_query.args)
            resp.body['slug'] = gdelt_query.slug
            resp.body = json.dumps(resp.body)
        else:
            resp.status = falcon.HTTP_204

    def on_post(self, req, resp):
        data = json.loads(s.unsign(req.get_param("signature", required=True)))

        print(data)

        gdelt_query = GdeltQueries.objects(cached=False, slug=data['slug']).first()
        gdelt_query.cached = True
        gdelt_query.put()


class HistoryResource:
    def on_get(self, req, resp):
        gdelt_query = GdeltQueries.objects(cached=False)
        resp.body = []

        for row in gdelt_query:
            resp.body.append(json.loads(row.args))

        resp.body = json.dumps(resp.body)


class S4Resource:
    def on_post(self, req, resp):
        print("get file:")
        payload = json.loads(s.unsign(req.stream.read()))
        f = open('static/results/' + payload['slug'], 'w')
        f.write(json.dumps(payload['data']))
        f.close()


api = falcon.API()
api.add_route('/api/data', DataResource())
api.add_route('/api/work', WorkResource())
api.add_route('/api/history', HistoryResource())
api.add_route('/api/result', S4Resource())
