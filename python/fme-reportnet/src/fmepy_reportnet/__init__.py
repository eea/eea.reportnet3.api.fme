try:
    """Additional log configuration for development purpose.
    Remember *not* to include reportnet3_logging.json when building
    """
    import os
    _log_config = os.path.join(os.path.realpath(os.path.dirname(__file__)), '..','..', '..', '..', 'reportnet3_logging.json')
    if os.path.exists(_log_config):
        import json
        import logging
        import logging.config
        logging.config.dictConfig(json.load(open(_log_config, 'r')))
except:
    pass

def FME_createReader(reader_type, src_keyword, mapping_file):
    """
    Reader entry point called by FME.
    FME expects this function name and signature.
    """
    from . import reader as reader_impl
    return reader_impl.Reportnet3Reader(reader_type, src_keyword, mapping_file)

def FME_createWriter(writer_type, dest_keyword, mapping_file):
    from fmegeneral.plugins import FMEMappingFileWrapper
    mf_wrapper = FMEMappingFileWrapper(mapping_file, dest_keyword, writer_type)
    if '1' == mf_wrapper.get('_REPORTNET_WRITER_VERSION'):
        from . import writer_v1 as writer
    else:
        from . import writer_v0 as writer
    return writer.Reportnet3Writer(writer_type, dest_keyword, mapping_file)
