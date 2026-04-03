from storage.db import Database

def main():
    db = Database()
    db.init_db()
    print("✅ 数据库初始化完成")

if __name__ == "__main__":
    main()