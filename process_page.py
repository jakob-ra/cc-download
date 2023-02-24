from bs4 import BeautifulSoup, SoupStrainer

keywords = pd.read_csv(f's3://{cfg["output_bucket"]}/keywords/url_keywords.txt', header=None).squeeze().to_list()

class PageProcessor:
    """Class for processing HTML pages"""
    def __init__(self, page: str, keywords: list = None):
        self.page = page
        self.keywords = keywords
        self.soup = BeautifulSoup(self.page, 'lxml', parse_only=SoupStrainer(['p', 'a']))

    def extract_texts(self):
        """Extract texts from HTML page (takes only texts between <p> tags)"""
        # get all text
        texts = [text for text in self.soup.stripped_strings]

        # cut off very long texts
        texts = [text[:10000] for text in texts]

        self.texts = texts
        return texts

    def get_keyword_mentioning_texts(self):
        """Return texts that mention any of the keywords"""
        keyword_mentioning_texts = [text for text in self.texts if
                                    any(keyword in text.casefold() for keyword in self.keywords)]

        self.keyword_mentioning_texts = keyword_mentioning_texts
        return keyword_mentioning_texts

    def extract_links(self):
        """Extract all links from HTML page"""
        # get all text
        links = [link.get('href') for link in self.soup.find_all('a') if link.get('href') is not None]
        links = [link.strip() for link in links if link.startswith('http')]
        # links = [link for link in links if not link.startswith(root_url)] # to get non-local links

        self.links = links
        return links

def process_page(page, keywords=keywords):
    """Process HTML page, return relevant paragraphs and non-local links"""
    processor = PageProcessor(page, keywords=keywords)
    processor.extract_texts()
    if keywords:
        processor.get_keyword_mentioning_texts()
    processor.extract_links()

    if keywords:
        return processor.texts, processor.keyword_mentioning_texts, processor.links
    else:
        return processor.texts, processor.links

# # test on https://www.siemens.com/global/en.html
# import requests
# keywords = ['k�nstliche intelligenz', 'k�nstlicher intelligenz', 'k�nstlichen intelligenz', 'artifizielle intelligenz', 'artifizielle intelligenz', 'artifiziellen intelligenz', 'artifizieller intelligenz', 'k�nstliches neuronales netz', 'handschrifterkennung', 'spracherkennung', 'gesichtserkennung', 'autonomes fahren', 'maschinelle �bersetzung', 'texterkennung', 'textgenerierung', 'sprachsteuerung', 'bilderkennung', 'computer vision', 'selbstfahrend', 'neuronales netz', 'maschinelles lernen', 'deep learning', 'verarbeitung nat�rlicher sprache', 'maschinelle wahrnehmung', 'k�nstliche allgemeine intelligenz', 'virtueller assistent', 'algorithmischer handel', 'autonome waffe', 'unbemanntes flugzeug', 'begleitroboter', 'pr�diktive analytik', 'automatischer assistent', 'verst�rkungslernen', 'deep belief network', 'spracherkennung', 'automatisches programmieren', 'k�nstliche kreativit�t', 'ki technologie', 'artificial intelligence', 'artificial neural network', 'handwriting recognition', 'speech recognition', 'face recognition', 'autonomous driving', 'machine translation', 'text recognition', 'text generation', 'voice control', 'image recognition', 'computer vision', 'self-driving', 'chatbot', 'neural network', 'neural networks', 'machine learning', 'deep learning', 'natural language processing', 'machine perception', 'artificial general intelligence', 'virtual assistant', 'image labeling', 'algorithmic trading', 'autonomous weapon', 'ucav', 'unmanned combat aerial vehicle', 'companion robot', 'sentiment analysis', 'predictive analytics', 'automated assistant', 'deepfake', 'reinforcement learning', 'deep belief network', 'speech recognition', 'automatic programming', 'artificial creativity', 'chatterbot', 'ai technology', 'emotion recognition', "l'intelligence artificielle", 'intelligence artificielle', 'r�seau neuronal artificiel', "reconnaissance de l'�criture manuscrite", 'reconnaissance vocale', 'reconnaissance faciale', 'conduite autonome', 'traduction automatique', 'reconnaissance de texte', 'g�n�ration de texte', 'commande vocale', "reconnaissance d'images", 'vision par ordinateur', 'r�seau neuronal', 'apprentissage automatique', 'apprentissage profond', 'traitement du langage naturel', 'perception automatique', 'intelligence g�n�rale artificielle', 'assistant virtuel', 'commerce algorithmique', 'arme autonome', 'avion sans pilote', "robot d'accompagnement", 'analyse pr�dictive', 'assistant automatique', 'apprentissage par renforcement', 'r�seau de croyances profondes', 'reconnaissance vocale', 'programmation automatique', 'technologie ai', 'cr�ativit� artificielle', 'intelligenza artificiale', "l'intelligenza artificiale", 'rete neurale artificiale', 'riconoscimento della scrittura', 'riconoscimento vocale', 'riconoscimento del volto', 'guida autonoma', 'traduzione automatica', 'riconoscimento del testo', 'generazione di testo', 'controllo vocale', "riconoscimento dell'immagine", 'visione artificiale', 'rete neurale', 'apprendimento automatico', 'apprendimento profondo', 'elaborazione del linguaggio naturale', 'percezione della macchina', 'intelligenza artificiale generale', 'assistente virtuale', 'trading algoritmico', 'arma autonoma', 'veicolo aereo senza equipaggio', 'robot compagno', 'analisi predittiva', 'assistente automatico', 'apprendimento con rinforzo', 'rete di credenza profonda', 'riconoscimento vocale', 'programmazione automatica', 'creativit� artificiale', 'tecnologia ai', "tecnologia dell'ai", 'kunstig intelligens', 'kunstige intelligens', 'artificiel intelligens', 'artificiel intelligent', 'kunstig intelligent', 'kunstigt neuralt netv�rk', 'h�ndskriftgenkendelse', 'talegenkendelse', 'ansigtsgenkendelse', 'autonom k�rsel', 'maskinovers�ttelse', 'tekstgenkendelse', 'tekstgenerering', 'stemmestyring', 'billedgenkendelse', 'neuralt netv�rk', 'maskinl�ring', 'dyb indl�ring', 'behandling af naturligt sprog', 'maskinopfattelse', 'kunstig generel intelligens', 'virtuel assistent', 'algoritmisk handel', 'autonomt v�ben', 'ubemandede luftfart�jer', 'f�lgesvend robot', 'pr�diktiv analyse', 'automatisk assistent', 'forst�rkningsindl�ring', 'dybe trosnetv�rk', 'talegenkendelse', 'automatisk programmering', 'kunstig kreativitet', 'ai-teknologi', 'ai teknologi', 'ai solutions', 'ai solution', 'ki l�sung', 'ki l�sungen', 'ai-l�sning', 'ai l�sning', 'solution ai', 'solutions ai', 'ai soluzione', 'ia soluzione', 'soluzione ai', 'soluzioni ai', 'soluzione ia', 'soluzioni ia', 'automated vehicle', 'v�hicule automatis�', 'v�hicule automatique', 'automatiseret k�ret�j', 'automatiserede k�ret�jer', 'automatisk k�ret�j', 'veicolo automatizzato', 'veicolo automatico', 'automatisiertes fahrzeug', 'automatisches fahrzeug', 'ai software', 'ki software', 'ai-powered', 'customizable ai', 'anpassbare ki', 'tilpasselig ai', 'ai personnalisable', 'ai personalizzabile', 'ia personnalisable', 'ia personalizzabile', 'language model', 'ai-based', 'ai based', 'ki-basiert', 'ki basiert', "bas� sur l'ai", "bas� sur l'ia", 'ai-baseret', 'ai applications', 'ai applikationer', 'ki anwendungen', "applications de l'ai", 'applicazioni ai', "applications de l'ia", 'applicazioni ia', 'ai/ml', 'ai expert', 'esperto di ai', 'esperto di ia', 'ki-experte', 'expert en ai', 'ai ekspert', 'ai use case', 'ai brugssituation', 'ki-anwendungsfall', "cas d'utilisation de l'ai", "cas d'usage de l'ai", "caso d'uso ai", "cas d'utilisation de l'ia", "cas d'usage de l'ia", "caso d'uso ia", 'ai algorithm', 'algoritmo ai', 'algoritmo ia', 'ki algorithmus', 'algorithme ai', "algorithme de l'ai", 'algoritmo ai', 'algoritmo di ai', 'algorithme ia', "algorithme de l'ia", 'algoritmo ia', 'algoritmo di ia', 'ai traffic', 'a. i.', 'a.i.', 'k.i.', 'k. i.', 'ai-models', 'ai-models', 'ki-modell', 'ki-modelle', 'modello ai', 'modello di ai', "mod�le de l'ai", "mod�le d'ai", 'mod�le en ai', 'modello ia', 'modello di ai', "mod�le de l'ia", "mod�le d'ia", 'mod�le en ia', 'ai startup', 'ai start-up', 'ki startup', 'ml models', 'ml model', 'power of ai', "aatalogue d'ia", 'ai-driven', 'ai platform', 'ki plattform', 'ia platform', "plate-forme de l'ia", "plate-forme de l'ia", 'piattaforma ai', 'piattaforma ia', 'piattaforma di ai', 'piattaforma di ia', 'deep tech', 'advanced ai', 'cutting edge ai', 'ai algorithms', 'ai annotation', 'automation ai', 'emotional ai', 'innovative ai', 'ai moderation', '.ai', 'ai facilitated', 'ai chip', 'text analysis', 'ai analytics', 'eye tracking', 'fortgeschrittene ki', 'modernste ki', 'ki algorithmen', 'automatisierte ki', 'emotionale ki', 'innovative ki', 'textanalyse', 'avanceret ai', 'avanceret ai', 'ai-algoritmer', 'automatisering ai', 'f�lelsesm�ssig ai', 'innovativ ai', 'tekstanalyse', 'ia avanc�e', 'ia de pointe', "algorithmes de l'ia", "automatisation de l'ia", 'ia �motionnel', 'ia innovante', 'analyse de texte', 'ia avanzata', "ia all'avanguardia", 'algoritmi ia', 'automazione ia', 'ia emozionale', 'ia innovativa', 'analisi del testo', '# corona', '# covid', '# pandemic', '# epidemic', '# virus', '# hygiene', '# lockdown', '# quarantine', '# outbreak', '# vaccine', '# health', '# office', '# remote']
# page = requests.get('https://www.siemens.com/global/en.html').content
# process_page(page, keywords=keywords)