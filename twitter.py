#!/usr/bin/env python3
import json
import re
import numpy as np
from datetime import datetime
from mpi4py import MPI


def processing(count_dict, tweet):
    ttext = tweet[0]

    for i in re.finditer('#', ttext):
        htag = ''

        for j in range(i.start(0), len(ttext)):
            if ttext[j] == '_' or 'a' <= ttext[j] <= 'z' or 'A' <= ttext[j] <= 'Z':
                htag += ttext[j]
            elif ttext[j] == '#':
                if '#' in htag:
                    break

                htag += ttext[j]
            else:
                break

        if htag == '#':
            continue

        htag = htag.lower()

        if htag in count_dict['hashtag']:
            count_dict['hashtag'][htag] += 1
        else:
            count_dict['hashtag'][htag] = 1

    lcode = tweet[1].split('-')[0]

    if lcode in count_dict['language']:
        count_dict['language'][lcode] += 1
    else:
        count_dict['language'][lcode] = 1


if __name__ == '__main__':
    ti = datetime.now()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    filename = 'tinyTwitter.json'

    language_codes = {
        'am': 'Amharic',
        'ar': 'Arabic',
        'bg': 'Bulgarian',
        'bn': 'Bengali',
        'bo': 'Tibetan',
        'bs': 'Bosnian',
        'ca': 'Catalan',
        'ckb': 'Sorani Kurdish',
        'cs': 'Czech',
        'cy': 'Welsh',
        'da': 'Danish',
        'de': 'German',
        'dv': 'Maldivian',
        'el': 'Greek',
        'en': 'English',
        'es': 'Spanish',
        'et': 'Estonian',
        'eu': 'Basque',
        'fa': 'Persian',
        'fi': 'Finnish',
        'fil': 'Filipino',
        'fr': 'French',
        'gu': 'Gujarati',
        'hi': 'Hindi',
        'hr': 'Croatian',
        'ht': 'Haitian',
        'hu': 'Hungarian',
        'hy': 'Armenian',
        'in': 'Indonesian',
        'is': 'Icelandic',
        'it': 'Italian',
        'iw': 'Hebrew',
        'ja': 'Japanese',
        'ka': 'Georgian',
        'km': 'Khmer',
        'kn': 'Kannada',
        'ko': 'Korean',
        'lo': 'Lao',
        'lt': 'Lithuanian',
        'lv': 'Latvian',
        'ml': 'Malayalam',
        'mr': 'Marathi',
        'msa': 'Malay',
        'my': 'Burmese',
        'ne': 'Nepali',
        'nl': 'Dutch',
        'no': 'Norwegian',
        'or': 'Oriya',
        'pa': 'Panjabi',
        'pl': 'Polish',
        'ps': 'Pashto',
        'pt': 'Portuguese',
        'ro': 'Romanian',
        'ru': 'Russian',
        'sd': 'Sindhi',
        'si': 'Sinhala',
        'sk': 'Slovak',
        'sl': 'Slovenian',
        'sr': 'Serbian',
        'sv': 'Swedish',
        'ta': 'Tamil',
        'te': 'Telugu',
        'th': 'Thai',
        'tl': 'Tagalog',
        'tr': 'Turkish',
        'ug': 'Uyghur',
        'uk': 'Ukrainian',
        'und': 'Undefined',
        'ur': 'Urdu',
        'vi': 'Vietnamese',
        'zh': 'Chinese'
    }

    count_data = {
        'hashtag': {},
        'language': {}
    }

    if rank == 0:
        segments = []

        with open(filename, encoding='utf-8') as file:
            for line in file:
                # noinspection PyBroadException
                try:
                    data = json.loads(line[:len(line) - 2])

                    segments.append((data['doc']['text'] + ' ' + data['doc']['user']['description'], data['doc']['metadata']['iso_language_code']))
                except:
                    continue

        if size > 1:
            segments = np.array_split(segments, size)
    else:
        segments = None

    if rank == 0 and size == 1:
        for s in segments:
            processing(count_data, s)
    else:
        for s in comm.scatter(segments, root=0):
            processing(count_data, s)

        count_data = comm.gather(count_data)

    if rank == 0 and size > 1:
        temp_data = {
            'hashtag': {},
            'language': {}
        }

        for i in count_data:
            if i is None:
                continue

            for key, val in i['hashtag'].items():
                if key in temp_data['hashtag']:
                    temp_data['hashtag'][key] += val
                else:
                    temp_data['hashtag'][key] = val

            for key, val in i['language'].items():
                if key in temp_data['language']:
                    temp_data['language'][key] += val
                else:
                    temp_data['language'][key] = val

        count_data = {
            'hashtag': temp_data['hashtag'],
            'language': temp_data['language']
        }

    if rank == 0:
        ctr = 1
        val10 = None
        rankings = 'Top 10 hashtags\n---------------\n'
        for key, val in sorted(count_data['hashtag'].items(), key=lambda x: (x[1], x[0]), reverse=True):
            if ctr > 10:
                if val != val10:
                    break

            rankings += f'{ctr}. {key}, {"{:,}".format(val)}\n'

            if ctr == 10:
                val10 = val

            ctr += 1

        ctr = 1
        val10 = None
        rankings += '\nTop 10 languages\n----------------\n'
        for key, val in sorted(count_data['language'].items(), key=lambda x: (x[1], x[0]), reverse=True):
            if ctr > 10:
                if val != val10:
                    break

            if key in language_codes:
                lname = language_codes[key]
            else:
                lname = 'Unknown'

            rankings += f'{ctr}. {lname} ({key}), {"{:,}".format(val)}\n'

            if ctr == 10:
                val10 = val

            ctr += 1

        print(rankings)

        tf = datetime.now()
        tdiff = (tf - ti).total_seconds()
        print(f'Time taken for execution: {int(tdiff // 60)}min{int(tdiff % 60)}sec')
