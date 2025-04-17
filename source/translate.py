##
# @package translate.py
# Translate text to english as default, using google translate requests.

import requests
from TFM.source.constants import ISO_CODES_NO_TRANSLATION,CHAR_LIMIT_TRANSLATE

##
# @brief _Descripció curta._
# @details _Descripció detallada si cal._
#
# @param texte (str): text of news to be translated.
# @param source_language (list, optional): List of languages of text news, from country. Defaults to ['auto'].
# @param dest_language (str, optional): Language to translate the text news. Defaults to 'en'.
#
# @return str: text translated.
def google_translate(texte: str, source_language: list = ['auto'], dest_language: str = 'en') -> str:
    """ Translate text to english as default, using google translate requests.

    Args:
        texte (str): text of news to be translated.
        source_language (list, optional): List of languages of text news, from country.
                                          Defaults to ['auto'].
        dest_language (str, optional): Language to translate the text news. Defaults to 'en'.

    Returns:
        str: text translated.
    """
    try:
        response_json_trans = None

        if len(source_language) == 1:
            s_lang = source_language[0]

        else:
            # In case more than one language in the country, will use auto, so translator have to
            # detect the language to translate to english.
            s_lang = 'auto'

        if s_lang not in ISO_CODES_NO_TRANSLATION: # Exclude [en, gb]

            # Google translate has a limit of 5000 characters but get only 2500
            if len(texte) > CHAR_LIMIT_TRANSLATE:
                texte = texte[0:CHAR_LIMIT_TRANSLATE]

            url = "https://clients5.google.com/translate_a/t?client=dict-chrome-ex&sl="\
                +s_lang+"&tl="+dest_language+"&q="+texte

            response_translate = requests.get(url)
            response_translate.raise_for_status()
            response_json_trans = response_translate.json()

            if s_lang == 'auto':
                # Check if lang detected are in languages of country send, because short sentence
                # of spanish was detected as portuguese language.
                if response_json_trans[0][1] in source_language:
                    return response_json_trans[0][0]
                else:
                    return None
            else:
                return response_json_trans[0]
        else:
            return texte

    except Exception as e:
        print('ERROR - Coudn\'t translate: ',e, '\n URL: ',url, '\n Response: ',
              response_json_trans)

