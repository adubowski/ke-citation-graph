# %%
import pandas as pd
import json
from tqdm import tqdm

from src.connection import Neo4jConnection
from src.process_data import insert_data,  iter_parse_stackexchange

# %%

port = 11003 # Check if is the case for your server!
PWD='knowledge'

conn = Neo4jConnection(uri="bolt://localhost:"+str(port),
                       user="neo4j",
                       pwd=PWD)


#%%

def add_papers(rows, batch_size=5000):
   # Adds paper nodes and relationships (:Author)-[:AUTHORED]-(:Paper), (:Paper)-[:REFERENCES]-(:Paper)
   query = '''
    // Create papers
    UNWIND $rows as paper
    MERGE (p:Paper {paperid: paper.id})
    ON CREATE SET
    p.title = paper.title,
    p.year = paper.year,
    p.n_citation = paper.n_citation,
    p.doi = paper.doi

    // Match authors
    WITH paper, p
    UNWIND  paper.authors AS author
    MERGE (a:Author {authorid: author.id})
    ON CREATE SET a.name = author.name
    MERGE (a)-[:AUTHORED]->(p)

    // Match references
    WITH paper, p
    UNWIND  paper.references AS refid
    MATCH (r:Paper {paperid:refid})
    MERGE (p)-[:REFERENCES]->(r)
    RETURN count(p:Paper) as total
   '''

   return insert_data(query, rows, batch_size)

# %%
conn.query('CREATE INDEX paper_id_index IF NOT EXISTS FOR (p:Paper) ON (p.paperid);')
conn.query('CREATE INDEX author_id_index IF NOT EXISTS FOR (a:Author) ON (a.authorid);')


# %%
file = "data/dblp_papers_v11.txt"
n_papers = 4107340 # Number of papers in the dataset
subset = ["id", "title", "year", "n_citation", "doi", "authors", "references"]

# TODO: Add tqdm max of the number of papers
def process_citation_data(file, n_papers, subset):
    with open(file, 'r') as f:
        for episode in tqdm(range(n_papers // 100000)):
            try:
                lines = 100000
                rows  = []
                for line in f:
                    rows.append(json.loads(line))
                    lines -= 1
                    if lines == 0: break
                df = pd.DataFrame(rows)
                add_papers(df[subset], 1000)
            except Exception as e:
                print(e)
                break

process_citation_data(add_papers, file, n_papers, subset)


# %%
def add_boards(board_list, batch_size=50):
    # Adds board nodes
    query = '''
    UNWIND $rows as board
    MERGE (b:Board {boardname: board})
    RETURN count(b:Board) as total
    '''
    return insert_data(conn, query, board_list, batch_size)

# %%
def add_posts(board, batch_size=50):
   # Adds post nodes
   query = '''
    // Create posts
    UNWIND $rows as post
    MERGE (p:Post {postid: post.Id})
    ON CREATE SET
    p.title = post.Title,
    p.text = post.Body,
    p.tags = post.Tags,
    p.score = post.Score

    WITH post, p
    UNWIND post.DOIs AS doi
    MATCH (r:Paper {doi:doi})
    MERGE (p)-[:REFERENCES]->(r)

    WITH post, p
    MATCH (b:Board {boardname:post.board})
    MERGE (p)-[:FROM_BOARD]->(b)
    RETURN count(p:Post) as total
   '''
   post_record_generator = iter_parse_stackexchange(board, 'Posts')
   return insert_data(conn, query, post_record_generator, batch_size)

# %%
def add_comments(board, batch_size=50):
    # Adds comment nodes
    query = '''
        // Create comments
        UNWIND $rows as comment
        MERGE (c:Comment {commentid: comment.Id})
        ON CREATE SET
        c.text = comment.Text

        // Match posts
        WITH comment, c
        MATCH (p:Post {postid: comment.PostId})
        MERGE (c)-[:RESPONDS_TO]->(p)

        // Match references
        WITH comment, c
        UNWIND comment.DOIs AS doi
        MATCH (r:Paper {doi:doi})
        MERGE (c)-[:REFERENCES]->(r)
        RETURN count(c:Comment) as total
    '''
    record_generator = iter_parse_stackexchange(board, 'Comments')
    return insert_data(conn, query, record_generator, batch_size)
# %%
def add_post_links(board, batch_size=50):
    # Adds post nodes
    query = '''
        // Create posts
        UNWIND $rows as post_link
        MATCH (p:Post {postid:post_link.PostId})
        MATCH (r:Post {postid:post_link.RelatedPostId})
        MERGE (p)-[:REFERENCES]->(r)
        RETURN count(p:Post) as total
    '''
    record_generator = iter_parse_stackexchange(board, 'PostLinks')
    return insert_data(conn, query, record_generator, batch_size)
# %%
conn.query('CREATE INDEX paper_doi_index IF NOT EXISTS FOR (p:Paper) ON (p.doi);')
conn.query('CREATE INDEX post_id_index IF NOT EXISTS FOR (p:Post) ON (p.postid);')
conn.query('CREATE INDEX comment_id_index IF NOT EXISTS FOR (c:Comment) ON (c.commentid);')
conn.query('CREATE INDEX board_index IF NOT EXISTS FOR (b:Board) ON (b.boardname);')

# %%
se_boards = [
    # "ai",
    # "cstheory",
    #  "datascience",
     "stats",
     ]
add_boards( se_boards, batch_size=1)

# %%
for board in se_boards:
    print("Board:", board)
    add_posts(board, batch_size=100)
    add_comments(board, batch_size=100)
    add_post_links(board, batch_size=100)


# %%
# %%

def query_for_use_case_1(conn: Neo4jConnection, post_title: str):
   query = '''
    MATCH (post1:Post)
    WHERE post1.title CONTAINS "''' + post_title + '''"
    MATCH (post1)-[:REFERENCES]->(paper:Paper)

    WITH paper, post1
    MATCH (post2: Post)-[:REFERENCES]->(paper)
    RETURN post1
   '''
   return conn.query(query)
query_for_use_case_1(conn, 'backprop')


