import streamlit as st
import mysql.connector
import requests
from datetime import datetime
import plotly.express as px 
import os
from dotenv import load_dotenv

# --- Environment Setup ---
load_dotenv()

# ──────────────────────────────────────────────
# 1. Database Connection
# ──────────────────────────────────────────────
def get_db_connection():
    try:
        return mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', 'admin'),
            database=os.getenv('DB_NAME', 'bookscape'),
            auth_plugin='mysql_native_password'
        )
    except Exception as e:
        st.error(f"Connection failed: {str(e)}")
        return None

def execute_query(query, params=None):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        if 'SELECT' in query.upper():
            return cursor.fetchall()
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        st.error(f"Database Error: {str(e)}")
        return None
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ──────────────────────────────────────────────
# 2. Core Functions
# ──────────────────────────────────────────────
def fetch_books(query, max_results=10):
    try:
        response = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={
                "q": query,
                "maxResults": max_results,
                "key": os.getenv('AIzaSyAOJRvXPARiwIu2zAdNae7ubiunTIaNkCY')
            }
        )
        data = response.json()
        return data.get('items', [])
    except Exception as e:
        st.error(f"API Error: {str(e)}")
        return []

def process_book(item):
    volume = item.get('volumeInfo', {})
    sale_info = item.get('saleInfo', {})
    
    return {
        'book_id': item['id'].replace("'", "''"),
        'title': volume.get('title', 'Unknown').replace("'", "''"),
        'authors': "|".join(volume.get('authors', ['Unknown'])).replace("'", "''"),
        'publisher': volume.get('publisher', 'Unknown').replace("'", "''"),
        'published_year': volume.get('publishedDate', '')[:4],
        'description': volume.get('description', '')[:500].replace("'", "''"),
        'isbn': next((id['identifier'].replace("'", "''") for id in volume.get('industryIdentifiers', [])
                     if id.get('type') in ['ISBN_10', 'ISBN_13']), ''),
        'page_count': volume.get('pageCount', 0),
        'categories': "|".join(volume.get('categories', ['Uncategorized'])).replace("'", "''"),
        'average_rating': float(volume.get('averageRating', 0)),
        'ratings_count': int(volume.get('ratingsCount', 0)),
        'price': float(sale_info.get('retailPrice', {}).get('amount', 0)),
        'currency': sale_info.get('retailPrice', {}).get('currencyCode', 'USD').replace("'", "''"),
        'thumbnail': volume.get('imageLinks', {}).get('thumbnail', '').replace("'", "''"),
        'import_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def store_books(books):
    if not books:
        return 0
    
    total = 0
    for book in books:
        # Ensure all required fields are present with defaults
        book_data = {
            'book_id': book.get('book_id', ''),
            'title': book.get('title', 'Unknown').replace("'", "''"),
            'authors': book.get('authors', 'Unknown').replace("'", "''"),
            'publisher': book.get('publisher', 'Unknown').replace("'", "''"),
            'published_year': book.get('published_year', '').replace("'", "''"),
            'description': book.get('description', '').replace("'", "''")[:500],  # Truncate if needed
            'isbn': book.get('isbn', '').replace("'", "''"),
            'page_count': book.get('page_count', 0),
            'categories': book.get('categories', 'Uncategorized').replace("'", "''"),
            'average_rating': book.get('average_rating', 0.0),
            'ratings_count': book.get('ratings_count', 0),
            'price': book.get('price', 0.0),
            'currency': book.get('currency', 'USD').replace("'", "''"),
            'thumbnail': book.get('thumbnail', '').replace("'", "''"),
            'import_timestamp': book.get('import_timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        }
        
        # Build the exact INSERT query matching your table structure
        query = f"""
        INSERT INTO books (
            book_id, title, authors, publisher, published_year,
            description, isbn, page_count, categories,
            average_rating, ratings_count, price, currency,
            thumbnail, import_timestamp
        ) VALUES (
            '{book_data['book_id']}', '{book_data['title']}', '{book_data['authors']}',
            '{book_data['publisher']}', '{book_data['published_year']}', '{book_data['description']}',
            '{book_data['isbn']}', {book_data['page_count']}, '{book_data['categories']}',
            {book_data['average_rating']}, {book_data['ratings_count']}, {book_data['price']},
            '{book_data['currency']}', '{book_data['thumbnail']}', '{book_data['import_timestamp']}'
        ) ON DUPLICATE KEY UPDATE
            title='{book_data['title']}',
            authors='{book_data['authors']}',
            publisher='{book_data['publisher']}'
        """
        
        try:
            result = execute_query(query)
            if result:
                total += 1
        except Exception as e:
            st.error(f"Failed to insert book {book_data['title']}: {str(e)}")
            st.write("Problematic query:", query)
    
    return total
	
# ──────────────────────────────────────────────
# 3. Page Components
# ──────────────────────────────────────────────
def show_home():
    st.write("""
    # BookScape Explorer Pro
    BookScape Explorer is your intelligent book discovery platform
     that searches across Google Books and analyzes your personal
     collection. Effortlessly find, save, and gain insights about 
        books that match your unique reading preferences.
    
    **Please use Navigate to access the sidebar**
    - Search for books (Basic or Advanced)
    - Analyze book trends
    - Querying the database
    - Statistics and Explore community features
    """)
    st.image("https://images.unsplash.com/photo-1544716278-ca5e3f4abd8c", width=300)
# ──────────────────────────────────────────────
# 4. Basic Search
# ──────────────────────────────────────────────
def basic_search():
    st.header("🔍 Basic Search")
    query = st.text_input("Search term", "python programming")
    max_results = st.slider("Max results", 1, 40, 10)
    
    if st.button("Search"):
        items = fetch_books(query, max_results)
        if items:
            books = [process_book(item) for item in items]
            st.success(f"Found {len(books)} books")
            
            for i, book in enumerate(books[:5]):
                st.write(f"{i+1}. **{book['title']}** by {book['authors']}")
            
        with st.spinner("💾 Saving to database..."):
                    try:
                        saved_count = store_books(books)
                        st.success(f"📚 Successfully saved {saved_count} books to database")
                    except Exception as e:
                        st.error(f"❌ Failed to save books: {str(e)}")                       
 
# ──────────────────────────────────────────────
# 5. Advanced Search
# ──────────────────────────────────────────────                      
def advanced_search():

    st.header("🔍 Advanced Search")
        
    # Verify database connection
    try:
        conn = get_db_connection()
        if not conn:
            st.error("❌ Failed to connect to database")
            return
        conn.close()
    except Exception as e:
        st.error(f"❌ Database connection error: {str(e)}")
        return

    with st.form("advanced_search_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            title = st.text_input("Title contains")
            author = st.text_input("Author name")
            genre = st.text_input("Genre/category")
            
        with col2:
            year = st.text_input("Publication year")
            min_rating = st.slider("Minimum rating", 0.0, 5.0, 3.0)
            min_pages = st.number_input("Minimum pages", 0, 5000, 0)
        
        search_clicked = st.form_submit_button("Search & Import")

    if search_clicked:
        # Step 1: Build Google Books API query
        api_query_parts = []
        if title: api_query_parts.append(title)
        if author: api_query_parts.append(f"inauthor:{author}")
        if genre: api_query_parts.append(f"subject:{genre}")
        if year: api_query_parts.append(f"after:{year}-01-01 before:{year}-12-31")
        
        api_query = "+".join(api_query_parts) if api_query_parts else "python"
        st.write(f"🔍 API Search Query: `{api_query}`")

        # Step 2: Fetch from Google Books API
        with st.spinner("🌐 Fetching books from Google Books API..."):
            try:
                items = fetch_books(api_query, 40)
                if not items:
                    st.warning("⚠️ No books found in Google Books API")
                    return
                
                st.success(f"✅ Found {len(items)} books in API results")

                # Display raw API results for debugging
                with st.expander("Show raw API results"):
                    st.json(items[:1])  # Show first item as sample

            except Exception as e:
                st.error(f"❌ API fetch failed: {str(e)}")
                return

        # Step 3: Process and filter books before storing
        with st.spinner("🔄 Processing and filtering books..."):
            books_to_store = []
            for item in items:
                try:
                    book = process_book(item)
                    # Apply client-side filters
                    if (book.get('average_rating', 0) >= min_rating and 
                        book.get('page_count', 0) >= min_pages):
                        books_to_store.append(book)
                except Exception as e:
                    continue
            
            if not books_to_store:
                st.warning("⚠️ No books matched your filters after processing")
                return
            
            st.success(f"📚 {len(books_to_store)} books passed filters")

        # Step 4: Store in MySQL
        with st.spinner("💾 Saving books to database..."):
            try:
                saved_count = store_books(books_to_store)
                st.success(f"✅ Saved {saved_count} books to database")
                
                # Verify storage
                verify_query = "SELECT COUNT(*) as count FROM books"
                verify_count = execute_query(verify_query)
                st.info(f"📊 Total books in database now: {verify_count[0]['count']}")
                
            except Exception as e:
                st.error(f"❌ Failed to save books: {str(e)}")
                return

        # Small delay to ensure records are committed


        # Step 5: Query MySQL with exact filters
        with st.spinner("🔍 Searching database..."):
            try:
                conditions = []
                params = {}
                
                if title:
                    conditions.append("title LIKE %(title)s")
                    params['title'] = f"%{title}%"
                if author:
                    conditions.append("authors LIKE %(author)s")
                    params['author'] = f"%{author}%"
                if genre:
                    conditions.append("categories LIKE %(genre)s")
                    params['genre'] = f"%{genre}%"
                if year:
                    conditions.append("published_year = %(year)s")
                    params['year'] = year
                
                conditions.append(f"average_rating >= {min_rating}")
                conditions.append(f"page_count >= {min_pages}")
                
                query = """
                SELECT 
                    book_id, title, authors, published_year,
                    average_rating, ratings_count, page_count,
                    categories, thumbnail
                FROM books
                """
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                query += " ORDER BY average_rating DESC LIMIT 50"
                
                st.write(f"📝 Database Query: `{query}`")
                st.write(f"🔢 Query Parameters: {params}")
                
                results = execute_query(query, params)
                
                if not results:
                    st.warning("⚠️ No books found in database after saving")
                    return
                
                st.success(f"🎉 Found {len(results)} matching books in database")
                
                # Display results
                for book in results:
                    with st.container():
                        col1, col2 = st.columns([1, 4])
                        
                        with col1:
                            if book.get('thumbnail'):
                                st.image(book['thumbnail'], width=100)
                            else:
                                st.write("No cover image")
                        
                        with col2:
                            st.subheader(book['title'])
                            st.write(f"**By:** {book['authors']}")
                            st.write(f"**Published:** {book.get('published_year', 'N/A')} | "
                                    f"**Rating:** ★{book.get('average_rating', 'N/A')} "
                                    f"({book.get('ratings_count', 0)} ratings)")
                            st.write(f"**Pages:** {book.get('page_count', 'N/A')} | "
                                    f"**Genres:** {book.get('categories', 'N/A')}")
                            
                            if st.button("Save Again", key=f"save_{book['book_id']}"):
                                if store_books([book]):
                                    st.success("Book saved again!")
                
                st.write(f"Showing {len(results)} of {len(results)} results")

            except Exception as e:
                st.error(f"❌ Database query failed: {str(e)}")

# ──────────────────────────────────────────────
# 6. Query Explorer
# ──────────────────────────────────────────────
def query_explorer():
    st.header("📊 Query Explorer")
    
    queries = {
        "1. Check Availability of eBooks vs Physical Books": """
            SELECT 
                SUM(is_ebook) as ebook_count,
                COUNT(*) - SUM(is_ebook) as physical_count,
                ROUND(SUM(is_ebook) * 100.0 / COUNT(*), 2) as ebook_percentage
            FROM books
        """,
        
        "2. Find the Publisher with the Most Books Published": """
            SELECT publisher, COUNT(*) as book_count
            FROM books
            WHERE publisher != 'Unknown'
            GROUP BY publisher
            ORDER BY book_count DESC
            LIMIT 1
        """,
        
        "3. Identify the Publisher with the Highest Average Rating": """
            SELECT publisher, AVG(average_rating) as avg_rating
            FROM books
            WHERE publisher != 'Unknown'
            GROUP BY publisher
            HAVING COUNT(*) > 5
            ORDER BY avg_rating DESC
            LIMIT 1
        """,
        
        "4. Get the Top 5 Most Expensive Books by Retail Price": """
            SELECT title, authors, price, currency
            FROM books
            WHERE price > 0
            ORDER BY price DESC
            LIMIT 5
        """,
        
        "5. Find Books Published After 2010 with at Least 500 Pages": """
            SELECT title, authors, published_year, page_count
            FROM books
            WHERE published_year > '2010' AND page_count >= 500
            ORDER BY page_count DESC
            LIMIT 100
        """,
        
        "6. List Books with Discounts Greater than 20%": """
            SELECT title, authors, 
                   price, 
                   (SELECT AVG(price) FROM books b2 WHERE b2.categories = books.categories) as avg_category_price,
                   ROUND((1 - (price / (SELECT AVG(price) FROM books b2 WHERE b2.categories = books.categories)) * 100, 2) as discount_percentage
            FROM books
            WHERE price > 0
            HAVING discount_percentage > 20
            ORDER BY discount_percentage DESC
            LIMIT 50
        """,
        
        "7. Find the Average Page Count for eBooks vs Physical Books": """
            SELECT 
                AVG(CASE WHEN is_ebook = 1 THEN page_count ELSE NULL END) as avg_ebook_pages,
                AVG(CASE WHEN is_ebook = 0 THEN page_count ELSE NULL END) as avg_physical_pages
            FROM books
            WHERE page_count > 0
        """,
        
        "8. Find the Top 3 Authors with the Most Books": """
            SELECT authors, COUNT(*) as book_count
            FROM books
            WHERE authors != 'Unknown'
            GROUP BY authors
            ORDER BY book_count DESC
            LIMIT 3
        """,
        
        "9. List Publishers with More than 10 Books": """
            SELECT publisher, COUNT(*) as book_count
            FROM books
            WHERE publisher != 'Unknown'
            GROUP BY publisher
            HAVING COUNT(*) > 10
            ORDER BY book_count DESC
        """,
        
        "10. Find the Average Page Count for Each Category": """
            SELECT 
                categories,
                AVG(page_count) as avg_page_count,
                COUNT(*) as book_count
            FROM books
            WHERE categories != '' AND page_count > 0
            GROUP BY categories
            ORDER BY avg_page_count DESC
            LIMIT 20
        """,
        
        "11. Retrieve Books with More than 3 Authors": """
            SELECT title, authors, 
                   LENGTH(authors) - LENGTH(REPLACE(authors, '|', '')) + 1 as author_count
            FROM books
            HAVING author_count > 3
            ORDER BY author_count DESC
            LIMIT 50
        """,
        
        "12. Books with Ratings Count Greater Than the Average": """
            SELECT title, authors, ratings_count, average_rating
            FROM books
            WHERE ratings_count > (SELECT AVG(ratings_count) FROM books WHERE ratings_count > 0)
            ORDER BY ratings_count DESC
            LIMIT 50
        """,
        
        "13. Books with the Same Author Published in the Same Year": """
            SELECT authors, published_year, COUNT(*) as book_count
            FROM books
            WHERE authors != 'Unknown' AND published_year != ''
            GROUP BY authors, published_year
            HAVING COUNT(*) > 1
            ORDER BY book_count DESC
            LIMIT 50
        """,
        
        "14. Books with a Specific Keyword in the Title": """
            SELECT title, authors, published_year
            FROM books
            WHERE title LIKE '%machine%'
            LIMIT 50
        """,
        
        "15. Year with the Highest Average Book Price": """
            SELECT published_year, AVG(price) as avg_price
            FROM books
            WHERE price > 0 AND published_year != ''
            GROUP BY published_year
            ORDER BY avg_price DESC
            LIMIT 1
        """,
        
        "16. Count Authors Who Published 3 Consecutive Years": """
            WITH author_years AS (
                SELECT 
                    authors,
                    published_year,
                    LAG(published_year, 1) OVER (PARTITION BY authors ORDER BY published_year) as prev_year,
                    LAG(published_year, 2) OVER (PARTITION BY authors ORDER BY published_year) as prev_prev_year
                FROM books
                WHERE authors != 'Unknown' AND published_year REGEXP '^[0-9]{4}$'
                GROUP BY authors, published_year
            )
            SELECT COUNT(DISTINCT authors) as authors_with_3_consecutive_years
            FROM author_years
            WHERE published_year = prev_year + 1 AND published_year = prev_prev_year + 2
        """,
        
        "17. Authors with Multiple Publishers in Same Year": """
            SELECT 
                authors,
                published_year,
                COUNT(DISTINCT publisher) as publisher_count,
                COUNT(*) as book_count
            FROM books
            WHERE authors != 'Unknown' 
              AND published_year != ''
              AND publisher != 'Unknown'
            GROUP BY authors, published_year
            HAVING COUNT(DISTINCT publisher) > 1
            ORDER BY book_count DESC
            LIMIT 50
        """,
        
        "18. Average Price Comparison: eBooks vs Physical": """
            SELECT 
                AVG(CASE WHEN is_ebook = 1 THEN price ELSE NULL END) as avg_ebook_price,
                AVG(CASE WHEN is_ebook = 0 THEN price ELSE NULL END) as avg_physical_price
            FROM books
            WHERE price > 0
        """,
        
        "19. Rating Outliers (2+ Standard Deviations)": """
            WITH rating_stats AS (
                SELECT 
                    AVG(average_rating) as mean,
                    STDDEV(average_rating) as stddev
                FROM books
                WHERE average_rating > 0
            )
            SELECT title, average_rating, ratings_count
            FROM books, rating_stats
            WHERE average_rating > 0
              AND (average_rating > mean + 2 * stddev OR average_rating < mean - 2 * stddev)
            ORDER BY ABS(average_rating - mean) DESC
            LIMIT 50
        """,
        
        "20. Top Rated Publisher (10+ Books)": """
            SELECT 
                publisher,
                AVG(average_rating) as avg_rating,
                COUNT(*) as book_count
            FROM books
            WHERE publisher != 'Unknown' AND average_rating > 0
            GROUP BY publisher
            HAVING COUNT(*) > 10
            ORDER BY avg_rating DESC
            LIMIT 1
        """
    }
    
    selected = st.selectbox("Choose query", list(queries.keys()))
    
    if st.button("Run Query"):
        results = execute_query(queries[selected])
        if results:
            # Display as table
            st.write(f"**Results ({len(results)}):**")
            
            # Create a HTML table
            table_html = "<table style='width:100%'><tr>"
            
            # Add headers
            for key in results[0].keys():
                table_html += f"<th>{key.replace('_', ' ').title()}</th>"
            table_html += "</tr>"
            
            # Add rows
            for row in results:
                table_html += "<tr>"
                for value in row.values():
                    table_html += f"<td>{value}</td>"
                table_html += "</tr>"
            
            table_html += "</table>"
            
            st.markdown(table_html, unsafe_allow_html=True)
            
            # Optional: Add download button
            if st.button("Download as CSV"):
                import csv
                from io import StringIO
                
                output = StringIO()
                writer = csv.DictWriter(output, fieldnames=results[0].keys())
                writer.writeheader()
                writer.writerows(results)
                
                st.download_button(
                    label="Download CSV",
                    data=output.getvalue(),
                    file_name=f"book_query_{selected.lower().replace(' ', '_')}.csv",
                    mime="text/csv"
                )
        else:
            st.warning("No results found for this query")

# ──────────────────────────────────────────────
# 7. Trend Analysis
# ──────────────────────────────────────────────
def trend_analysis():
    st.header("📈 Trend Analysis")
    
    tab1, tab2 = st.tabs(["By Year", "By Rating"])
    
    with tab1:
        years = execute_query("SELECT DISTINCT published_year FROM books WHERE published_year != '' ORDER BY published_year DESC")
        if years:
            selected_years = st.multiselect("Select years", [y['published_year'] for y in years])
            if selected_years:
                query = f"""
                SELECT published_year, COUNT(*) as count
                FROM books
                WHERE published_year IN ({','.join(f"'{y}'" for y in selected_years)})
                GROUP BY published_year
                ORDER BY published_year
                """
                data = execute_query(query)
                if data:
                    st.write("Publications per year:")
                    for row in data:
                        st.write(f"{row['published_year']}: {row['count']} books")
    
    with tab2:
        query = """
        SELECT 
            CASE
                WHEN average_rating >= 4.5 THEN '4.5+'
                WHEN average_rating >= 4.0 THEN '4.0-4.5'
                WHEN average_rating >= 3.5 THEN '3.5-4.0'
                ELSE 'Below 3.5'
            END as rating_range,
            COUNT(*) as count
        FROM books
        GROUP BY rating_range
        ORDER BY rating_range
        """
        results = execute_query(query)
        if results:
            st.write("Books by rating range:")
            for row in results:
                st.write(f"{row['rating_range']}: {row['count']} books")

# ──────────────────────────────────────────────
# 8. Data Insights
# ──────────────────────────────────────────────
def data_insights():
    st.header("💡 Data Insights")
    
    st.write("**Price Distribution**")
    try:
        # Fixed query - backticks around reserved keyword
        price_data = execute_query("""
            SELECT 
                CASE
                    WHEN price = 0 THEN 'Free'
                    WHEN price < 10 THEN '0-10'
                    WHEN price < 20 THEN '10-20'
                    ELSE '20+'
                END AS `price_range`,
                COUNT(*) as count
            FROM books
            WHERE price IS NOT NULL
            GROUP BY `price_range`
            ORDER BY 
                CASE `price_range`
                    WHEN 'Free' THEN 0
                    WHEN '0-10' THEN 1
                    WHEN '10-20' THEN 2
                    ELSE 3
                END
        """)
        
        if price_data:
            st.write("### Books by Price Range")
            fig = px.bar(price_data, x='price_range', y='count', 
                         labels={'price_range': 'Price Range', 'count': 'Number of Books'})
            st.plotly_chart(fig)
        else:
            st.warning("No price data available")
    except Exception as e:
        st.error(f"Error loading price data: {str(e)}")

    st.write("---")
    
    st.write("**Rating Distribution**")
    try:
        rating_data = execute_query("""
            SELECT 
                CASE
                    WHEN average_rating >= 4.5 THEN '4.5+ Stars'
                    WHEN average_rating >= 4.0 THEN '4.0-4.5 Stars'
                    WHEN average_rating >= 3.5 THEN '3.5-4.0 Stars'
                    WHEN average_rating >= 3.0 THEN '3.0-3.5 Stars'
                    ELSE 'Below 3.0'
                END AS rating_range,
                COUNT(*) as count
            FROM books
            WHERE average_rating IS NOT NULL
            GROUP BY rating_range
            ORDER BY 
                CASE rating_range
                    WHEN '4.5+ Stars' THEN 0
                    WHEN '4.0-4.5 Stars' THEN 1
                    WHEN '3.5-4.0 Stars' THEN 2
                    WHEN '3.0-3.5 Stars' THEN 3
                    ELSE 4
                END
        """)
        
        if rating_data:
            st.write("### Books by Rating")
            fig = px.pie(rating_data, names='rating_range', values='count')
            st.plotly_chart(fig)
        else:
            st.warning("No rating data available")
    except Exception as e:
        st.error(f"Error loading rating data: {str(e)}")
		
# ──────────────────────────────────────────────
# 9. Community and Statistics
# ──────────────────────────────────────────────		
def community():
    st.header("👥 Community Hub")
    
    with st.expander("Discussion Forum"):
        post = st.text_area("Share your thoughts")
        if st.button("Post"):
            st.success("Posted to community board!")
    
    with st.expander("Statistics"):
        stats = execute_query("SELECT COUNT(*) as total_books FROM books")
        if stats:
            st.write(f"Total books in database: {stats[0]['total_books']}")
        st.write("Active users: 42")

# ──────────────────────────────────────────────
# 10. Main App
# ──────────────────────────────────────────────
def main():
    # Initialize database
    execute_query("""
    CREATE TABLE IF NOT EXISTS books (
        book_id VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255),
        authors TEXT,
        publisher VARCHAR(255),
        published_year VARCHAR(10),
        description TEXT,
        isbn VARCHAR(20),
        page_count INT,
        categories TEXT,
        average_rating FLOAT,
        ratings_count INT,
        price FLOAT,
        currency VARCHAR(10),
        thumbnail TEXT,
        import_timestamp DATETIME
    )
    """)
    
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Menu", [
        "Home", 
        "Basic Search", 
        "Advanced Search", 
        "Query Explorer",
        "Trend Analysis",
        "Data Insights",
        "Community and statistics"
    ])
    
    if page == "Home":
        show_home()
    elif page == "Basic Search":
        basic_search()
    elif page == "Advanced Search":
        advanced_search()
    elif page == "Query Explorer":
        query_explorer()
    elif page == "Trend Analysis":
        trend_analysis()
    elif page == "Data Insights":
        data_insights()
    elif page == "Community and statistics":
        community()

if __name__ == "__main__":
    main()