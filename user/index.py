import sqlite3
import os

from user.jwt_handler import generate_token

def validate_username_exists_orm(username, database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            
            query = session.query(user_model).filter(user_model.user_name == username).first()
            return query is not None
    except Exception as e:
        print(f"Error validating username '{username}' in database '{db_name}': {e}")
        return False


def user_register_orm(username, password, email, phone, database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            

            # Create a new UserInfo object
            new_user = user_model(
                user_name=username,
                user_type='user',
                user_password=password,
                user_email=email,
                user_phone=phone,
                user_validated=0
            )
            
            # Add the new user to the session
            session.add(new_user)
            
            # Commit the transaction
            session.commit()
            return 1
    except Exception as e:
        print(f"Error registering user '{username}' in database '{db_name}': {e}")
        session.rollback()
        return 0


def user_login_orm(username, password, database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            

            # Query the user by username
            user = session.query(user_model).filter(user_model.user_name == username).first()
            status = 0
            user_type = None
            token = None
            
            if user:
                # If username exists
                stored_password = user.user_password
                user_validated = user.user_validated
                user_type = user.user_type
                
                # Check if the password matches
                if stored_password != password:
                    status = 2
                elif user_validated == 0:
                    # If user is not validated
                    status = 1
                else:
                    # If password matches and user is validated
                    status = 0
                    # Generate JWT token
                    token = generate_token(username, user_type)
            else:
                # If username does not exist
                status = 3
            
            return status, token
    except Exception as e:
        print(f"Error logging in user '{username}' in database '{db_name}': {e}")
        return 3, None


def get_all_user_orm(database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            

            # Query all users
            users = session.query(user_model).all()
            results = [(user.user_name, user.user_type, user.user_email, user.user_phone, user.user_validated) for user in users]
            return results
    except Exception as e:
        print(f"Error fetching all users from database '{db_name}': {e}")
        return []


def get_user_by_name_orm(username, database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            

            # Query users by name
            users = session.query(user_model).filter(user_model.user_name.like(f'%{username}%')).all()
            results = [(user.user_name, user.user_type, user.user_email, user.user_phone, user.user_validated) for user in users]
            return results
    except Exception as e:
        print(f"Error fetching users by name '{username}' from database '{db_name}': {e}")
        return []


def change_user_status_orm(username, status, database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            

            # Update user status
            user = session.query(user_model).filter(user_model.user_name == username).first()
            if user:
                user.user_validated = status
                session.commit()
                return 1
            else:
                return 0
    except Exception as e:
        print(f"Error changing status for user '{username}' in database '{db_name}': {e}")
        session.rollback()
        return 0


def delete_user_orm(username, database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            

            # Delete user
            user = session.query(user_model).filter(user_model.user_name == username).first()
            if user:
                session.delete(user)
                session.commit()
                return 1
            else:
                return 0
    except Exception as e:
        print(f"Error deleting user '{username}' from database '{db_name}': {e}")
        session.rollback()
        return 0


def edit_user_orm(username, user_type, email, phone, database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            

            # Edit user details
            user = session.query(user_model).filter(user_model.user_name == username).first()
            if user:
                user.user_type = user_type
                user.user_email = email
                user.user_phone = phone
                session.commit()
                return 1
            else:
                return 0
    except Exception as e:
        print(f"Error editing user '{username}' in database '{db_name}': {e}")
        session.rollback()
        return 0


def reset_password_orm(username, password, database_manager, user_model):
    db_name = 'user'
    try:
        with database_manager.get_session(db_name) as session:
            

            # Reset user password
            user = session.query(user_model).filter(user_model.user_name == username).first()
            if user:
                user.user_password = password
                session.commit()
                return 1
            else:
                return 0
    except Exception as e:
        print(f"Error resetting password for user '{username}' in database '{db_name}': {e}")
        session.rollback()
        return 0



# sqlite3版本
def validate_username_exists(username,repo_abs_path):
    database_path = os.path.join(repo_abs_path, 'database','user.db')
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    query_string = '''
        SELECT * FROM UserInfo
        WHERE user_name = ?
    '''
    cursor.execute(query_string, (username,))
    results = cursor.fetchall()
    ret = len(results) > 0
    cursor.close()
    conn.close()
    return ret


def user_register(username, password, email, phone, repo_abs_path):
    database_path = os.path.join(repo_abs_path, 'database','user.db')
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # 创建一个带参数的SQL插入语句
    sql = '''INSERT INTO UserInfo (
        user_name, user_type, user_password, user_email, user_phone, user_validated
    ) VALUES (?, ?, ?, ?, ?, ?)'''

    ret = 1
    # 执行SQL语句
    try:
        # 这里我们假设password已经是MD5加密后的字符串
        cursor.execute(sql, (username, 'user', password, email, phone, 0))
        
        # 提交事务
        conn.commit()
    except sqlite3.Error as e:
        ret = 0
    finally:
        # 关闭Cursor和Connection:
        if conn:
            cursor.close()
            conn.close()

    return ret


def user_login(username, password, repo_abs_path):
    database_path = os.path.join(repo_abs_path, 'database','user.db')
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # 查询用户名是否存在
    cursor.execute('SELECT user_password, user_validated, user_type FROM UserInfo WHERE user_name = ?', (username,))
    result = cursor.fetchone()

    status = 0
    user_type = None
    token = None
    if result:
        # 如果用户名存在
        stored_password, user_validated, user_type = result

        # 检查密码是否匹配
        if stored_password != password:
            status = 2
        elif user_validated == 0:
            # 如果用户未验证
            status = 1
        else:
            # 如果密码匹配且用户已验证
            status = 0
            # 生成JWT token
            token = generate_token(username, user_type)
    else:
        # 如果用户名不存在
        status = 3
    
    cursor.close()
    conn.close()
    return status, token


def get_all_user(repo_abs_path):
    database_path = os.path.join(repo_abs_path, 'database','user.db')
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute('SELECT user_name, user_type, user_email, user_phone, user_validated FROM UserInfo')
    results = cursor.fetchall()

    cursor.close()
    conn.close()
    return results


def get_user_by_name(username, repo_abs_path):
    database_path = os.path.join(repo_abs_path, 'database','user.db')
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute('SELECT user_name, user_type, user_email, user_phone, user_validated FROM UserInfo WHERE user_name LIKE ?', ('%' + username + '%',))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


def change_user_status(username, status, repo_abs_path):
    database_path = os.path.join(repo_abs_path, 'database','user.db')
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    ret = 1
    try:
        cursor.execute('UPDATE UserInfo SET user_validated = ? WHERE user_name = ?', (status, username))
        conn.commit()
    except sqlite3.Error as e:
        ret = 0
        print('Error:', e)
    finally:
        if conn:
            cursor.close()
            conn.close()
    return ret


def delete_user(username, repo_abs_path):
    database_path = os.path.join(repo_abs_path, 'database','user.db')
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    ret = 1
    try:
        cursor.execute('DELETE FROM UserInfo WHERE user_name = ?', (username,))
        conn.commit()
    except sqlite3.Error as e:
        ret = 0
        print('Error:', e)
    finally:
        if conn:
            cursor.close()
            conn.close()

    return ret


def edit_user(username, user_type, email, phone, repo_abs_path):
    database_path = os.path.join(repo_abs_path, 'database','user.db')
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    ret = 1
    try:
        cursor.execute('UPDATE UserInfo SET user_type = ?, user_email = ?, user_phone = ? WHERE user_name = ?', (user_type, email, phone, username))
        conn.commit()
    except sqlite3.Error as e:
        ret = 0
        print('Error:', e)
    finally:
        if conn:
            cursor.close()
            conn.close()

    return ret


def reset_password(username, password, repo_abs_path):
    database_path = os.path.join(repo_abs_path, 'database','user.db')
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    ret = 1
    try:
        cursor.execute('UPDATE UserInfo SET user_password = ? WHERE user_name = ?', (password, username))
        conn.commit()
    except sqlite3.Error as e:
        ret = 0
        print('Error:', e)
    finally:
        if conn:
            cursor.close()
            conn.close()
    return ret
