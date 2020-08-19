import os
import csv
import codecs
import re
from bs4 import BeautifulSoup
from nltk import sent_tokenize
from nltk import word_tokenize


class CorpusReader:

    def __init__(self, root=None):
        self.root = root

    def ids(self, count=None, categories=None):
        pass

    def categories(self, categories=None, count=None):
        pass

    def stat(self, ids=None, categories=None):
        pass

    def abspath(self, ids):
        pass

    def docs(self, ids=None, categories=None):
        pass

    def sizes(self, ids=None, categories=None):
        pass


class FileCorpusReader(CorpusReader):

    def __init__(self, root=None):
        CorpusReader.__init__(self, root)

    def categories(self, categories=None, count=None):
        """
        Returns a list of categories
        :param categories:
        :param count: maximum list size
        :return: list of strings or empty list
        """
        CorpusReader.categories(self, count)
        counter = 0
        cats = []
        for root, dirs, files in os.walk(self.root):
            if root == self.root:
                continue
            if count is not None and counter == count:
                break
            cats.append(os.path.basename(root))
            counter += 1

        return cats

    def category(self, file_id):
        cat, file_name = os.path.split(file_id)
        return cat

    def ids(self, count=None, categories=None):
        CorpusReader.ids(self, count, categories)
        counter = 0
        ids = []
        for root, dirs, files in os.walk(self.root):
            if root == self.root:
                continue

            if count is not None and counter == count:
                break

            cat_str = os.path.basename(root)
            if categories is not None and cat_str not in categories:
                continue

            for file_name in files:
                if count is not None and counter == count:
                    return ids

                ids.append(os.path.join(cat_str, file_name))
                counter += 1

        return ids

    def resolve(self, ids, categories=None):
        """
        Returns a list of ids or categories depending on what is passed
        to each internal corpus reader function.
        """
        if ids is not None and categories is not None:
            raise ValueError("Specify ids or categories, not both")
        if categories is not None:
            return self.ids(categories=categories)
        if ids is not None:
            if isinstance(ids, list):
                return ids
            else:
                return [ids]

        return self.ids()

    def id_to_abspath(self, doc_id):
        """
        Returns document's absolute path in file system
        :param doc_id: document id, string like 'category/filename'
        :return: absolute path
        """
        proc_folder = doc_id.split(os.path.sep)[0]
        file, ext = os.path.splitext(os.path.basename(doc_id))
        src_folder = os.path.join(self.root, proc_folder)
        return os.path.join(src_folder, file) + ext

    def abspath(self, ids):

        for fid in ids:
            cat_name, file_name = os.path.split(fid)
            yield os.path.join(self.root, cat_name, file_name)

    def docs(self, ids=None, categories=None):
        """
        Returns absolute path of an file in corpus
        :param ids:
        :param categories:
        :return:
        """
        # Resolve the ids and the categories
        fileids = self.resolve(ids, categories)

        for path in self.abspath(fileids):
            yield path

    def stat(self, ids=None, categories=None):

        fileids = self.resolve(ids, categories)

        # Categories count
        if categories is None:
            cat = []
            for fid in fileids:
                cat.append(self.category(fid))
            cat_count = len(set(cat))
        else:
            cat_count = len(set(categories))

        # Extensions
        ext = []
        for path in self.abspath(fileids):
            file_name, file_ext = os.path.splitext(path)
            file_ext = file_ext.lower()[1:]
            ext.append(file_ext)
        ext = list(set(ext))

        corp_size = sum([x for x in self.sizes()])

        return {
            'files': len(fileids),
            'categories': cat_count,
            'ext': ext,
            'file_size': corp_size
        }

    def sizes(self, ids=None, categories=None):
        """
        Returns a list of tuples, the file id and size on disk of the file.
        :param ids: text ids
        :param categories: text categories
        :return: file sizes
        """
        fileids = self.resolve(ids, categories)

        for path in self.abspath(fileids):
            yield os.path.getsize(path)

    def write_file_csv(self, folder=None):
        # write csv
        # folder,file,ext,size,readable, path
        corpus_folder = self.root if folder is None else folder

        with open(os.path.join(corpus_folder, 'index.csv'), mode='w') as file:
            csv_fields = ['folder', 'file', 'ext', 'size', 'path']
            writer = csv.DictWriter(file, fieldnames=csv_fields, delimiter=',')

            writer.writeheader()
            file_count = 0

            for root, dirs, files in os.walk(corpus_folder):

                if root == corpus_folder:
                    continue

                base_folder = os.path.split(root)[1]

                for f in files:
                    file_count += 1

                    file_path = os.path.join(root, f)
                    file_size = os.path.getsize(file_path)

                    file_name, file_ext = os.path.splitext(f)

                    file_ext = file_ext[1:].lower().strip()

                    writer.writerow({
                        'folder': base_folder,
                        'file': f,
                        'ext': file_ext,
                        'size': file_size,
                        'path': file_path
                    })

            file.close()


class HTMLCorpusReader(FileCorpusReader):

    def __init__(self, root=None):
        FileCorpusReader.__init__(self, root)
        self.stop_words = ['.', ',', '”', '„', ',', '-', '(', ')', ':', '«', '»', ';', '–', '{', '}', '™']
        self.stemmer = None
        self.clean_text = False
        self.language = 'english'

    def docs(self, ids=None, categories=None):
        """
        Returns content of an HTML document
        """
        # Resolve the fileids and the categories
        fileids = self.resolve(ids, categories)

        # Create a generator, loading one document into memory at a time
        for path in self.abspath(fileids):
            with codecs.open(path, 'r') as f:
                yield f.read()

    def paras(self, ids=None, categories=None):
        """
        Uses BeautifulSoup to parse the paragraphs from the HTML.
        """
        # Tags to extract as paragraphs from the HTML text
        tags = [
            'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7', 'p', 'li', 'dd', 'dt'
        ]

        clean_text = self.clean_text

        for html in self.docs(ids, categories):
            soup = BeautifulSoup(html, 'html.parser')
            for element in soup.find_all(tags):
                raw_text = element.text
                text = clean_paragraph(raw_text) if clean_text else raw_text
                if text:
                    yield text
            soup.decompose()

    def sents(self, ids=None, categories=None):
        """
        Extract sentences from the paragraphs
        """
        clean_text = self.clean_text
        language = self.language

        for paragraph in self.paras(ids, categories):
            for sentence in sent_tokenize(paragraph, language):
                text = clean_sentence(sentence) if clean_text else sentence
                if text:
                    yield text

    def words(self, ids=None, categories=None):
        """
        Extract words from the sentences
        """
        for sentence in self.sents(ids, categories):
            for word in self.text_to_words(sentence):
                yield word

    def text_to_words(self, text):

        clean_text = self.clean_text
        language = self.language

        for token in word_tokenize(text, language):
            if clean_text:
                word = clean_word(token, self.stop_words)
                if self.stemmer is not None:
                    word = self.stemmer.stem_word(word)
            else:
                word = token

            if word:
                yield word


def clean_paragraph(text):
    clean_text = text.strip()

    # Del quotes and etc
    clean_text = re.sub(r"""['’"`�]""", '', clean_text)
    clean_text = re.sub(r"""([0-9])([\u0400-\u04FF]|[A-z])""", r"\1 \2", clean_text)
    clean_text = re.sub(r"""([\u0400-\u04FF]|[A-z])([0-9])""", r"\1 \2", clean_text)
    # clean_text = re.sub(r"""[\-.,:+*/_]""", ' ', clean_text)

    # Format п.п.1 п.5 ст. 41 Закону України
    clean_text = re.sub(r"\s{1,}п.\s{1,}", ' п.', clean_text)
    clean_text = re.sub(r"\s{1,}ч.\s{1,}", ' ч.', clean_text)
    clean_text = re.sub(r"ст.\s{1,}", 'ст.', clean_text)
    clean_text = re.sub(r"\s{1,}п.\s{0,}п.\s{0,}", ' п.п.', clean_text)

    clean_text = re.sub(r"\s{1,}№\s{1,}", ' №', clean_text)

    # т.ч.ПДВ
    clean_text = re.sub(r"\s{1,}т.ч.\s{0,}", ' т.ч. ', clean_text)

    # links
    clean_text = re.sub('https?://\S+|www\.\S+', '', clean_text)
    clean_text = re.sub('http?://\S+|www\.\S+', '', clean_text)

    clean_text = re.sub('…{1,}', '…', clean_text)

    # underscores
    clean_text = re.sub('_{1,}', '', clean_text)

    # underscores
    clean_text = re.sub('_{1,}', '', clean_text)

    # multi points
    clean_text = re.sub('\.{1,}', '.', clean_text)

    # slash
    clean_text = re.sub('/', ' ', clean_text)
    clean_text = re.sub('\\\\', ' ', clean_text)

    # id=3303
    clean_text = re.sub('\s{0,}=\s{0,}', ' = ', clean_text)

    # буд.ХХ
    clean_text = re.sub('\s{0,}буд[.{1}]\s{0,}', 'буд. ', clean_text)

    # All characters are non alfa
    if re.match("^[0-9  .,-:+*_;\\/]+$", clean_text):
        clean_text = ''

    # Too small for paragraph
    if len(clean_text) <= 3:
        clean_text = ''

    return clean_text


def clean_sentence(text):
    clean_text = text.strip()
    # All characters are non alfa
    if re.match("^[0-9  .,-:+*_;\\/]+$", clean_text):
        clean_text = ''

    return clean_text


def clean_word(text, stop_words=None):
    clean_text = text.strip()
    clean_text = clean_text.lower()
    if stop_words and clean_text in stop_words:
        clean_text = ''
    return clean_text