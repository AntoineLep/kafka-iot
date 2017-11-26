class KafkaIotException(Exception):
    """Raise generic kafka-iot exception"""

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, message):
        self.message = message

    # ------------------------------------------------------------------------------------------------------------------
    def __str__(self):
        return "/!\ KAFKA IOT EXCEPTION: " + self.message
