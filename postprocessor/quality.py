def sanity_check(meta):
    return all(
        key in meta.keys() for key in [
        'camel_case',
        'punctuations',
        'symbols',
        'oovs',
        'pos_x',
        'lexical_density',
        'gunning_fog',
        'avg_sentence_length'
    ]
    )

def get_data(temp_meta):
    keys = ['punctuations', 'symbols', 'oovs', 'pos_x']
    for key in keys:
        temp_meta[f'{key}_ratio'] = temp_meta[f'{key}'] / temp_meta['words']
    return temp_meta

def get_filtered(meta):
    mask_cc = {'LOW': meta['camel_case'] > 10, 'HIGH': meta['camel_case'] < 3}
    mask_punct = {
        'LOW': (meta['punctuations_ratio'] > 0.4) | (meta['punctuations_ratio'] < 0.1),
        'HIGH': (meta['punctuations_ratio'] > 0.1) & (meta['punctuations_ratio'] <= 0.35)
    }
    mask_symb = {'LOW': meta['symbols_ratio'] > 0.01, 'HIGH': meta['symbols_ratio'] == 0}

    mask_oovs = {'LOW': meta['oovs_ratio'] > 0.15, 'HIGH': meta['oovs_ratio'] < 0.05}
    mask_x = {'LOW': meta['pos_x_ratio'] > 0.07, 'HIGH': meta['pos_x_ratio'] < 0.01}

    mask_dens = {
        'LOW': (meta['lexical_density'] > 0.8) | (meta['lexical_density'] < 0.2),
        'HIGH': (meta['lexical_density'] < 0.6) & (meta['lexical_density'] > 0.4)
    }
    mask_fog = {'LOW': meta['gunning_fog'] > 14, 'HIGH': meta['gunning_fog'] < 10}
    mask_sent = {
        'LOW': (meta['avg_sentence_length'] > 35) | (meta['avg_sentence_length'] < 5),
        'HIGH': (meta['avg_sentence_length'] > 10) & (meta['avg_sentence_length'] < 26)
    }
    if (
                mask_symb['LOW'] | mask_punct['LOW'] | mask_cc['LOW']
        ):
        return 'LOW'
    elif (
                mask_x['LOW'] | mask_oovs['LOW']
        ):
        return 'LOW'
    elif (
                mask_sent['LOW'] | mask_fog['LOW'] | mask_dens['LOW']
        ):
        return 'LOW'

    elif (
                mask_symb['HIGH'] & mask_punct['HIGH'] & mask_cc['HIGH']
        ) | (
                mask_x['HIGH'] & mask_oovs['HIGH']
        ) | (
                mask_sent['HIGH'] & mask_fog['HIGH'] & mask_dens['HIGH']
        ):
        return 'HIGH'
    else:
        return 'MEDIUM'
    
def get_doc_quality(meta):
    temp_meta = meta.copy()
    try:
        temp_meta = get_data(temp_meta)
        temp_meta['quality'] = get_filtered(temp_meta)
        meta['quality'] = temp_meta['quality']
    except:
        meta['quality'] = 'LOW'
    return meta
