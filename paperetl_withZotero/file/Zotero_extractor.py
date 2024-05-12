import os.path
import sqlite3
import pandas as pd
from icecream import ic

Zotero_database = 'zotero-library-path'


def extract_authors(connz):
    df_item_creators = pd.read_sql_query('SELECT * FROM itemCreators', connz)
    df_creators = pd.read_sql_query('SELECT * FROM creators', connz)
    df_combined = pd.merge(df_item_creators, df_creators, on='creatorID')
    itemIDs = []
    authors = []
    for itemID, df in df_combined.groupby('itemID'):
        df = df.sort_values(by='orderIndex')
        res = df.apply(lambda x: f'{x.lastName}, {x.firstName}', axis=1).to_numpy()
        res = ';'.join(res)
        itemIDs.append(itemID)
        authors.append(res)
    author_df = pd.DataFrame(data={'itemID':itemIDs, 'authors':authors})
    return author_df


def createValueFrame(itemID, con):
    return pd.read_sql_query(f"""
    SELECT
    i.itemID,
    idv.value,
    f.fieldName,
    i.key
    FROM itemDataValues AS idv
    JOIN itemData as id ON idv.valueID=id.valueID
    JOIN items as i ON id.itemID=i.itemID
    JOIN fields as f ON id.fieldID=f.fieldID
    WHERE i.itemID=={itemID}
    """, con)


def createMatchFrame(key, con):
    return pd.read_sql_query(f"""
    SELECT
    c.collectionID,
    i.itemID as 'i.itemID',
    ia.parentItemID,
    i.key,
    idv.value as 'fieldValue',
    fc.fieldName
    FROM collections AS c
    JOIN collectionItems as ci ON c.collectioniD=ci.collectionID
    JOIN itemAttachments as ia ON ia.parentItemID=ci.itemID
    JOIN items as i ON i.itemID=ia.itemID
    JOIN itemData as id ON id.itemID=i.itemID
    JOIN itemDataValues as idv ON idv.valueID=id.valueID
    JOIN fieldsCombined as fc ON id.fieldID=fc.fieldID
    WHERE i.key=='{key}'
    """, con)


def extractItemIDFromMF(mf):
    return mf.iloc[0,2]


def key_extractor(path):
    dirname = os.path.dirname(path)
    return dirname.split(os.sep)[-1]


def createZoteroSql(dirname, con):
    #get item key from dirname
    mf = createMatchFrame(dirname, con)
    itemKey = extractItemIDFromMF(mf)
    #get values based on item key
    vf = createValueFrame(itemKey, con)
    df_authors = extract_authors(con)
    df_combined_4 = pd.merge(vf, df_authors, on='itemID')
    #only keep relevant columns
    df_combined_short = df_combined_4.loc[:, ['itemID', 'value', 'fieldName', 'authors']]
    return df_combined_short


def create_metadata_dict_from_df(df):
    array = df.loc[:,['fieldName', 'value']].to_numpy()
    metadata_dict = {f:v for f,v in array}
    metadata_dict['authors']=df.loc[0,'authors']
    return metadata_dict


def extract_zotero_metadata_to_dictionary(path):
    with sqlite3.connect(Zotero_database) as connz:
        dirname = key_extractor(path)
        df_db = createZoteroSql(dirname, connz)
        metadata_dict = create_metadata_dict_from_df(df_db)
    return metadata_dict


def parse_zotero_metadata(metadata_dict):
    title, published, publication, authors, affiliations, affiliation, reference = (
        None,
        None,
        None,
        None,
        None,
        None,
        None
    )
    for key, item in metadata_dict.items():
        if key=='title':
            title=item
        elif key=='date':
            published=item
        elif key=='authors':
            authors=item
        elif key=='publicationTitle':
            publication=item
        elif key=='DOI':
            reference=f'https://doi.org/{item}'

    return (title, published, publication, authors, affiliations, affiliation, reference)

