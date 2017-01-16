from bs4 import BeautifulSoup as bs
import os, sys, logging, glob
import cssutils as cu
import json
import collections
import gc
import pdb
import re
import string
import multiprocessing


ferr = open("errors_in_scraping.log", "w")


def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el


def parse_page(in_file, urlid, old_doc=None):
    """ parameters:
            - in_file: file to read raw_data from
            - url_id: id of each page from file_name """
    page = open(in_file, 'r')
    with open(in_file, 'r') as infile:
        page_raw = infile.read()
    soup = bs(page)
    text = soup.text
    rec = {
        "id": (lambda _: urlid, [None]),
        #"text": parse_text(soup),
        'header': (parse_header, [soup]),
        "title": (parse_title, [soup]),
        "meta": (parse_meta, [soup, 'content']),
        "meta_property": (parse_meta, [soup, 'property']),
        "meta_name": (parse_meta, [soup, 'name']),
        "lists": (parse_list_items, [soup]),
        "links": (parse_links, [soup]),
        "links_visible_text": (parse_links_text, [soup]),
        "links_parents": (parse_links_parents, [soup]),
        "images": (parse_images, [soup]),
        "tags": (parse_tags, [soup]),
        's_lines': (text.count, ['\n']),
        's_spaces': (text.count, [' ']),
        's_tabs': (text.count, ['\t']),
        's_braces': (text.count, ['{']),
        's_bracesr': (text.count, ['}']),
        's_brackets': (text.count, ['[']),
        's_bracketsr': (text.count, [']']),
        's_words': (lambda x: len(re.split('\s+', x)), [text]),
        's_length': (len, [text]),
        'a_eq': (text.count, ['=']),
        'a_at': (text.count, ['@']),
        'a_ret': (text.count, ['\r']),
        'a_scolon': (text.count, [';']),
        'a_colon': (text.count, [':']),
        'a_comma': (text.count, [',']),
        'a_qmark': (text.count, ['?']),
        'a_exmark': (text.count, ['!']),
        'a_and': (text.count, ['&']),
        'a_dot': (text.count, ['.']),
        'a_plus': (text.count, ['+']),
        'a_minus': (text.count, ['-']),
        'a_star': (text.count, ['*']),
        'a_dollar': (text.count, ['$']),
        'a_percent': (text.count, ['%']),
        'a_slash': (text.count, ['/']),
        'a_bslash': (text.count, ['\\']),
        'a_dquote': (text.count, ['"']),
        'a_quote': (text.count, ['\'']),
        'a_underscore': (text.count, ['_']),
        'a_lt': (text.count, ['<']),
        'a_gt': (text.count, ['>']),
        'a_hash': (text.count, ['#']),
        'a_html_comment': (text.count, ['<!--']),
        'b_js_comment_1': (text.count, ['//']),
        'b_js_comment_n': (text.count, ['/*']),
        'b_id': (text.count, ['id']),
        'b_js_var': (text.count, ['var ']),
        'b_js_fun': (text.count, ['function']),
        'b_rbracket': (text.count, ['(']),
        'b_rbracket_pair': (text.count, ['()']),
        'b_incr': (text.count, ['++']),
        'b_js_ext': (text.count, ['.js']),
        'b_for': (text.count, ['for']),
        'b_if': (text.count, ['if']),
        'b_else': (text.count, ['else']),
        'b_while': (text.count, ['while']),
        'b_ajax': (text.count, ['ajax']),
        'b_post': (text.count, ['post']),
        'b_break': (text.count, ['break']),
        'b_continue': (text.count, ['continue']),
        'b_encoding': (lambda _: soup.original_encoding, [None]),
        'c_lines': (page_raw.count, ['\n']),
        'c_spaces': (page_raw.count, [' ']),
        'c_tabs': (page_raw.count, ['\t']),
        'c_braces': (page_raw.count, ['{']),
        'c_brackets': (page_raw.count, ['[']),
        'c_words': (lambda x: len(re.split('\s+', x)), [page_raw]),
        'c_length': (len, [page_raw]),
        'c_eq': (page_raw.count, ['=']),
        'c_at': (page_raw.count, ['@']),
        'c_ret': (page_raw.count, ['\r']),
        'c_scolon': (page_raw.count, [';']),
        'c_colon': (page_raw.count, [':']),
        'c_comma': (page_raw.count, [',']),
        'c_qmark': (page_raw.count, ['?']),
        'c_exmark': (page_raw.count, ['!']),
        'c_and': (page_raw.count, ['&']),
        'c_dot': (page_raw.count, ['.']),
        'c_plus': (page_raw.count, ['+']),
        'c_minus': (page_raw.count, ['-']),
        'c_star': (page_raw.count, ['*']),
        'c_dollar': (page_raw.count, ['$']),
        'c_percent': (page_raw.count, ['%']),
        'c_slash': (page_raw.count, ['/']),
        'c_bslash': (page_raw.count, ['\\']),
        'c_dquote': (page_raw.count, ['"']),
        'c_quote': (page_raw.count, ['\'']),
        'c_underscore': (page_raw.count, ['_']),
        'c_lt': (page_raw.count, ['<']),
        'c_gt': (page_raw.count, ['>']),
        'c_hash': (page_raw.count, ['#']),
        'c_html_comment': (page_raw.count, ['<!--']),
        'c_js_comment_1': (page_raw.count, ['//']),
        'c_js_comment_n': (page_raw.count, ['/*']),
        'c_id': (page_raw.count, ['id']),
        'c_js_var': (page_raw.count, ['var ']),
        'c_js_fun': (page_raw.count, ['function']),
        'c_rbracket': (page_raw.count, ['(']),
        'c_rbracketr': (page_raw.count, [')']),
        'c_rbracket_pair': (page_raw.count, ['()']),
        'c_incr': (page_raw.count, ['++']),
        'c_js_ext': (page_raw.count, ['.js']),
        'c_for': (page_raw.count, ['for']),
        'c_if': (page_raw.count, ['if']),
        'c_else': (page_raw.count, ['else']),
        'c_while': (page_raw.count, ['while']),
        'c_ajax': (page_raw.count, ['ajax']),
        'c_post': (page_raw.count, ['post']),
        'c_break': (page_raw.count, ['break']),
        'c_continue': (page_raw.count, ['continue']),
        'd_upper': (lambda x: len(re.findall(r'[A-Z]', x)), [page_raw]),
        'd_lower': (lambda x: len(re.findall(r'[a-z]', x)), [page_raw]),
        'd_digit': (lambda x: len(re.findall(r'[0-9]', x)), [page_raw]),
        'd_special': (lambda x: len(re.findall(r'[^\w\s]', x)), [page_raw]),
        "attr": (parse_attributes, [soup, 'names']),
        }

    for l in string.ascii_letters + string.digits:
        rec['d_%s' % l] = (page_raw.count, [l])

    doc = {} if old_doc is None else old_doc

    missing_keys = set(rec.keys()) - set(doc.keys())

    for k in missing_keys:
        func, args = rec[k]
        doc[k] = func(*args)

    #doc = derivative_features(doc)  # Takes too much disk space

    return doc


def derivative_features(doc):
    features = [f for f in doc.keys() if re.match('^[sabcd]_', f)]
    features.sort()

    for i, f1 in enumerate(features):
        for f2 in features[:i]:
            if isinstance(doc[f1], (int, long, float)) and isinstance(doc[f2], (int, long, float)):
                doc['i_%s_vs_%s' % (f1, f2)] = doc[f2] / float(max(doc[f1], 1e-15))

    return doc


def parse_tags(soup):
    tags = ['']
    for tag in soup.find_all(True):
        tags.append(tag.name)

    return ' '.join(filter(None, tags))


def parse_attributes(soup, kind):
    attributes = ['']

    for tag in soup.find_all(True):
        try:
            if kind == 'all':
                attributes.append(tag.attrs.items())
            elif kind == 'names':
                attributes.append(tag.attrs.keys())
        except Exception as e:
            print e

    attributes = list(flatten(attributes))

    return ' '.join(filter(None, attributes))


def parse_text(soup):
    """ parameters:
            - soup: beautifulSoup4 parsed html page
        out:
            - textdata: a list of parsed text output by looping over html paragraph tags
        note:
            - could soup.get_text() instead but the output is more noisy """
    textdata = ['']

    for text in soup.find_all('p'):
        try:
            t = text.text.encode('ascii', 'ignore').strip()
            textdata.append(t.replace('\n', ' ').replace('\r', ''))
        except Exception:
            continue

    return filter(None, textdata)


def parse_list_items(soup):
    textdata = ['']

    for text in soup.find_all('li'):
        try:
            t = text.text.encode('ascii', 'ignore').strip()
            textdata.append(t.replace('\n', ' ').replace('\r', ''))
        except Exception:
            continue

    return filter(None, textdata)


def parse_meta(soup, field):
    textdata = ['']

    for meta in soup.find_all('meta'):
        try:
            t = meta[field].encode('ascii', 'ignore').strip()
            textdata.append(t.replace('\n', ' ').replace('\r', ''))
        except Exception:
            continue

    return filter(None, textdata)


def parse_header(soup):
    textdata = ['']

    for tag in ['h1', 'h2', 'h3', 'h4', 'strong']:
        for text in soup.find_all(tag):
            try:
                t = text.text.encode('ascii', 'ignore').strip()
                textdata.append(t.replace('\n', ' ').replace('\r', ''))
            except Exception:
                continue

    return filter(None, textdata)


def parse_title(soup):
    title = ['']

    try:
        title.append(soup.title.string.encode('ascii', 'ignore').strip())
    except Exception:
        return title

    return filter(None, title)


def parse_links_text(soup):
    linkdata = ['']

    for link in soup.find_all('a'):
        try:
            linkdata.append(link.string.encode('ascii', 'ignore').strip())
        except Exception:
            continue

    return filter(None, linkdata)


def parse_links_parents(soup):
    linkdata = ['']

    for link in soup.find_all('a'):
        try:
            linkdata.append(link.parent.name)
        except Exception:
            continue

    return filter(None, linkdata)


def parse_links(soup):
    linkdata = ['']

    for link in soup.find_all(['a', 'link']):
        try:
            linkdata.append(str(link.get('href').encode('ascii', 'ignore')))
        except Exception:
            continue
    for link in soup.find_all('script'):
        try:
            linkdata.append(str(link.get('src').encode('ascii', 'ignore')))
        except Exception:
            continue

    return filter(None, linkdata)


def parse_images(soup):
    """ parameters:
            - soup: beautifulSoup4 parsed html page
        out:
            - imagesdata: a list of parsed image names by looping over html img tags """
    imagesdata = ['']

    for image in soup.findAll("img"):
        try:
            imagesdata.append("%(src)s" % image)
        except Exception:
            continue

    return filter(None, imagesdata)


def process_bucket(b, inFolder, outputDirectory, reuseDirectory):
    print 'Starting bucket %d' % b
    json_array = []

    fIn = glob.glob(inFolder + '/%d/*raw*' % b)
    out_file = os.path.join(outputDirectory, 'chunk%d.json' % b)
    feedsjson = open(out_file, mode='w')
    f_old = open(os.path.join(reuseDirectory, 'chunk%d.json' % b), 'r') if reuseDirectory is not None else None

    for idx, filename in enumerate(fIn):
        if idx % (200 + 31 * b) == 0:
            print "Thread %d: Saving %d" % (b, idx)
            for entry in json_array:
                json.dump(entry, feedsjson)
                feedsjson.write('\n')
            json_array = []
            gc.collect()

        filenameDetails = filename.split("/")
        urlId = filenameDetails[-1].split('_')[0]

        old_doc = None if f_old is None else json.loads(f_old.readline())

        if old_doc is not None and old_doc['id'] != urlId:
            print 'Thread %d: Record mismatch in line %d' % (b, idx + 1)

        try:
            doc = parse_page(filename, urlId, old_doc)
        except Exception as e:
            ferr.write('Thread ' + str(b) + ": parse error with reason : "+str(e)+" on page "+urlId+"\n")
            continue

        json_array.append(doc)

    # Save rest of records
    for entry in json_array:
        json.dump(entry, feedsjson)
        feedsjson.write('\n')

    feedsjson.close()
    if f_old is not None:
        f_old.close()
    print 'Thread %d: SAVING BUCKET FINISHED' % b
    print len(json_array)


def main(argv):
    """ parameters:
                - argv: sys args from the command line that consist of:
                            <label_file> <input_raw_dir> <output_directory>
                * input_raw_dir: directory to read raw input html files
                * output_directory: directory to save processed html files

        note:
                - this will loop over all raw_files and create processed ouput for
                  a give site_id IF input data for that id exists, otherwise it will
                  skip it """
    if len(argv) < 3:
        print " Usage: python crawler.py <input_raw_dir> <output_directory>"
        return

    else:
        inFolder = argv[1]
        outputDirectory = argv[2]
        reuseDirectory = argv[3] if len(argv) >= 4 else None

        if not os.path.exists(inFolder):
            print inFolder, " does not exist"
            return

        if reuseDirectory is not None and not os.path.exists(reuseDirectory):
            print reuseDirectory, " does not exist"
            return

        if not os.path.exists(outputDirectory):
            os.makedirs(outputDirectory)

        cu.log.setLevel(logging.CRITICAL)

        jobs = []
        for b in range(6):
            t = multiprocessing.Process(target=process_bucket, args=(b, inFolder, outputDirectory, reuseDirectory))
            jobs.append(t)

        for j in jobs:
            j.start()
        for j in jobs:
            j.join()

    print "Scraping completed .. There may be errors .. check log at errors_in_scraping.log"


if __name__ == "__main__":
   main(sys.argv)
