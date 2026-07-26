Things remaining:

1. Logging
2. /representaives route
3. Automatic mess menu parsing
4. Add instructions in readme to setup env vars and get service_account.
5. documentation ( auto generated )




Fix all the warnings and todos and bugs in code

added_by_email is taken from the request body instead of the auth token in add_event, add_announcement, add_outlet, add_bus, so a caller can spoof it. It should be derived from the validated Firebase token like the other handlers.
The token -> email validation block is duplicated a lot of times across handlers, extract it into a shared middleware.
bid_timestamp/claim_timestamp are client-settable (only defaulted, not overwritten server-side), so a caller can spoof the time. Set them in add_bid/claim_found instead.
edit_buy_sell/edit_lost_found cannot edit images. re-save from base64_images the way add_* does.
