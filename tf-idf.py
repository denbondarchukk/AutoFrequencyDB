from nltk.tokenize import word_tokenize
import collections
import math
import re


# попередня обробка тексту: переведення в нижній регістр і видалення небуквених символів
def preprocess_text(text):
    words = word_tokenize(text.lower())
    filtered_words = [token for token in words if re.match(r'\w+', token)]
    return filtered_words


# обчислення TF для кожного слова
def compute_tf(corpus):
    tf_text = collections.Counter(corpus)
    for word in tf_text:
        tf_text[word] = 1 + math.log10(tf_text[word])
    return tf_text


# обчислення IDF для кожного унікального слова
def compute_idf(corpus):
    num_documents = len(corpus)
    idf_dict = {}
    all_words = set(word for document in corpus for word in document)

    for word in all_words:
        doc_count = sum(1 for document in corpus if word in document)
        idf_dict[word] = math.log10(num_documents / doc_count)
    return idf_dict


# обчислення TF-IDF для всіх термінів у корпусі текстів
def compute_tfidf(corpus):
    idf_dict = compute_idf(corpus)
    tf_idf_dict = {}

    for document in corpus:
        tf_dict = compute_tf(document)
        for word, tf in tf_dict.items():
            tf_idf_dict[word] = tf * idf_dict.get(word, 0.0)

    sorted_tfidf = sorted(tf_idf_dict.items(), key=lambda item: item[1], reverse=True)

    return sorted_tfidf


# зчитування вмісту файлів і повернення корпусу оброблених текстів
def read_files(file_names):
    corpus = []
    for file_name in file_names:
        with open(file_name, 'r', encoding='utf-8') as file:
            text = file.read()[:30000]  # беремо 30000 символів "з запасом"
            tokens = preprocess_text(text)[:20000]  # відбираємо 20000 словоформ
            corpus.append(tokens)
    return corpus


# використання
file_names = ['Istoria_zaporizkykh_kozakiv_Tom1.txt', 'Biblia_Staryi_zapovit.txt']
corpus = read_files(file_names)
top_terms = compute_tfidf(corpus)[:100]

for idx, (term, score) in enumerate(top_terms, 1):
    print(f"{idx}) {term}: {score:.4f}")
