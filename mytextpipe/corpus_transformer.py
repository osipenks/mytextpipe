
class CorpusTransformer:

    def __init__(self, corpus_source, corpus_target, ids=None):
        self.source = corpus_source
        self.target = corpus_target
        self.ids = ids

    def transform(self, ids=None, transformer=None, pre_processor=None, post_processor=None, debug=False):
        count = 0
        for doc_id in ids:

            preprocessing = True
            if pre_processor is not None:
                preprocessing = pre_processor(corp=self.source, doc_id=doc_id)

            if preprocessing:
                new_doc_id = transformer(source=self.source, target=self.target, doc_id=doc_id)
                if new_doc_id:
                    post_processor(corp=self.target, doc_id=new_doc_id)

            if debug:
                count += 1
                print('{} {} to {}'.format(count, doc_id, new_doc_id))
