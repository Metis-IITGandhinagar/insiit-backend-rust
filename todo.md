Things remaining:

1. Logging
2. /representaives route
3. Automatic mess menu parsing
4. Add instructions in readme to setup env vars and get service_account.
5. documentation ( auto generated )




Fix all the warnings and todos and bugs in code

edit_buy_sell/edit_lost_found overwrite img_urls with whatever base64_images holds, so
editing an entry without re-uploading its images silently deletes them. Keep the existing
urls when base64_images is empty.

Making add_* idempotent needs an Idempotency-Key header plus a table of seen keys, since
announcements/events/buy-sell/lost-found/outlets have no natural key. add_bid and
claim_found are done (deduped per user), add_admin and add_mess_menu were already
idempotent, and add_bus/add_representative could use a UNIQUE constraint + ON CONFLICT.
