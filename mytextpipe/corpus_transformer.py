class CorpusTransformer:
    """
    Pipeline for arbitrary corpora transformation and conversion

    Use a fixed sequence of steps to process data in corpus.
    Each step uses: transformation function, corpus to apply this function
    and arbitrary parameters, if needed

    Transformation function get dictionary as argument
    with predefined keys 'doc_id' and 'corpus', and other user defined args

    Usage example:

    tr = CorpusTransformer(file_corp, html_corp, doc_ids)
    tr.transform([
                ('file_to_html', file_to_html, file_corp),
                ('html_post_processing', html_corp_postpoces, html_corp)
                ],

                {'file_to_html': {'target': html_corp}},

                 debug=True)

    # Transformation function
    def file_to_html(args_dict):
        doc_id = args_dict['doc_id']
        source = args_dict['corpus']
        target = args_dict['target']
        ...

    """

    def __init__(self, source, target, ids=None):
        self.source = source
        self.target = target
        self.ids = ids if ids else []

    def transform(self, steps, step_args=None, ids=None, debug=False):

        # For each step get args as dict {step_name: step_params}
        func_args = dict()
        for step_name, step_func, corp in steps:
            init_args = {'doc_id': '', 'corpus': corp}
            func_args[step_name] = init_args
            if step_args:
                func_args[step_name] = step_args[step_name] if step_name in step_args else init_args

        # Iterate through steps running step function with args
        doc_ids = ids if ids else self.ids
        for doc_id in doc_ids:
            prev_step = ''
            for step_name, step_func, corp in steps:
                if not prev_step:
                    func_args[step_name]['doc_id'] = doc_id
                    func_args[step_name]['corpus'] = corp
                else:
                    func_args[step_name]['doc_id'] = func_args[prev_step]['doc_id']
                    func_args[step_name]['corpus'] = corp

                step_func(func_args[step_name])
                prev_step = step_name
