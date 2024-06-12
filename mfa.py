import pyotp
import qrcode

def generate_secret():
    # Generate a new secret key
    # secret = pyotp.random_base32()
    secret = "2A7JV552P6G6WEE3TU6I525CW6SA5JCK"
    print(f"Your new secret key: {secret}")
    return secret

def generate_qr_code(secret, email):
    # Generate a TOTP URI
    # Associate this secret key with user email address
    totp_uri = pyotp.totp.TOTP(secret).provisioning_uri(name=email, issuer_name="MyApp")
    
    # Generate and save the QR code
    qr = qrcode.make(totp_uri)
    qr.save("qrcode.png")
    print(f"QR code generated and saved as qrcode.png")

def validate_totp(secret):
    # User enters the TOTP code from their authenticator app
    user_provided_code = input("Enter the TOTP code: ")
    
    # Validate the TOTP code
    totp = pyotp.TOTP(secret)
    if totp.verify(user_provided_code):
        print("The code is valid!")
    else:
        print("The code is invalid!")

if __name__ == "__main__":
    email = "user@example.com"  # Replace with the user's email
    secret = generate_secret()
    # generate_qr_code(secret, email)
    validate_totp(secret)
