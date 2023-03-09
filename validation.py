"""
Validate data
"""
import math


def validate_pincode(pincode: int) -> dict:
    """
    Check if the pincode is valid or not

    Test cases:
        Valid pincode - 533344
        Invalid pincodes, Not 6 digits - abcded, 53334d, 5333

    Args:
        pincode (int): Pincode to validate

    Returns:
        dict: Response with status and content
    """
    response = {}
    response['status'] = False
    try:
        pincode_str_int = int(pincode)
        pincode_length = math.floor(math.log10(pincode_str_int)) + 1
        if pincode_length == 6:
            response['status'] = True
        else:
            response['content'] = "Pincode must contain only 6 digits."
        return response
    except ValueError:
        response['content'] = "Invalid pincode."
        return response
