from xml.etree import ElementTree
import boto3
import gzip

AWS_PROFILE_NAME = 'darwin'
SCHEDULE_S3_BUKCET = 'darwin.xmltimetable'
SCHEDULE_S3_PREFIX = 'PPTimetable'


def fetch_schedule_xml():
    session = boto3.Session(profile_name=AWS_PROFILE_NAME)
    s3 = session.client('s3')
    list_response = s3.list_objects_v2(
        Bucket=SCHEDULE_S3_BUKCET,
        Prefix=SCHEDULE_S3_PREFIX,
    )
    objects = list_response['Contents']
    count = len(list_response['Contents'])

    # Work backwards until we find the latest v8
    latest_schedule_object = list_response['Contents'][count-1]
    for item in reversed(objects):
        if 'v8' in item['Key']:
            latest_schedule_object = item
            break

    print('Fetching schedule from S3: ' + latest_schedule_object['Key'])
    get_response = s3.get_object(
        Bucket=SCHEDULE_S3_BUKCET,
        Key=latest_schedule_object['Key']
    )

    schedule_gzipped = get_response['Body'].read()
    schedule_raw = gzip.decompress(schedule_gzipped)

    return ElementTree.fromstring(schedule_raw)


def get_schedules():
    tree = fetch_schedule_xml()
    print(tree)
    schedules = []
    for journey in tree:
        base_schedule = {
            'rid': journey.attrib.get('rid'),
            'uid': journey.attrib.get('uid'),
            'trainId': journey.attrib.get('trainId'),
            'ssd': journey.attrib.get('ssd'),
            'toc': journey.attrib.get('toc')
        }

        for location in journey:
            pta = location.attrib.get('pta')
            ptd = location.attrib.get('ptd')
            if pta is None and ptd is None:
                continue

            schedule = base_schedule.copy()
            schedule.update({
                'tpl': location.attrib.get('tpl'),
                'plat': location.attrib.get('plat'),
                'pta': location.attrib.get('pta'),
                'ptd': location.attrib.get('ptd')
            })
            schedules.append(schedule)

    return schedules
