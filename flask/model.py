from database import db


class Users(db.Model):
    __tablename__ = 'Users'

    email = db.Column(db.String(40), primary_key=True, unique=True, nullable=False)
    password = db.Column(db.String(120), unique=False, nullable=False)
    authenticated = db.Column(db.Boolean, default=False)
    
    def is_active(self):
        """True, as all users are active."""
        return True

    def get_id(self):
        """Return the email address to satisfy Flask-Login's requirements."""
        return self.email

    def is_authenticated(self):
        """Return True if the user is authenticated."""
        return self.authenticated

    def is_anonymous(self):
        """False, as anonymous users aren't supported."""
        return False

    def __repr__(self):
        return '<User %r>' % self.email

    def __init__(self, email, password):
        self.email = email
        self.password = password