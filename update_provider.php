<?php
/**
 * Simple endpoint to update YouTube URL in mm_jmultimedia.provider
 *
 * Usage (GET):
 *   update_provider.php?id=14434&youtube_url=https://www.youtube.com/watch?v=%2FXXXX
 *
 * Response:
 *   { "ok": true, "id": 14434, "youtube_url": "https://www.youtube.com/watch?v=XXXX" }
 *
 * SECURITY:
 * - Consider restricting by an auth token or IP allowlist in production.
 * - Use HTTPS only.
 */

header('Content-Type: application/json; charset=utf-8');

function respond($code, array $data) {
    http_response_code((int) $code);
    echo json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    exit;
}

// Validate input
$id = isset($_GET['id']) ? (int) $_GET['id'] : 0;
$youtubeUrl = isset($_GET['youtube_url']) ? trim($_GET['youtube_url']) : '';

if ($id <= 0 || $youtubeUrl === '') {
    respond(400, [
        'ok' => false,
        'error' => 'missing_parameters',
        'message' => 'Required GET params: id (int), youtube_url (string)'
    ]);
}

// Basic URL validation
if (!filter_var($youtubeUrl, FILTER_VALIDATE_URL)) {
    respond(400, [
        'ok' => false,
        'error' => 'invalid_url',
        'message' => 'youtube_url is not a valid URL'
    ]);
}

// DB credentials (provided)
$dbHost = 'localhost';
$dbName = '';
$dbUser = '';
$dbPass = '';
$dbCharset = 'utf8mb4';

$conn = mysqli_init();
mysqli_options($conn, MYSQLI_OPT_INT_AND_FLOAT_NATIVE, 1);

if (!mysqli_real_connect($conn, $dbHost, $dbUser, $dbPass, $dbName)) {
    respond(500, [
        'ok' => false,
        'error' => 'db_connection_failed',
        'message' => mysqli_connect_error(),
    ]);
}

if (!mysqli_set_charset($conn, $dbCharset)) {
    respond(500, [
        'ok' => false,
        'error' => 'db_charset_failed',
        'message' => mysqli_error($conn),
    ]);
}

$sql = "UPDATE mm_jmultimedia SET provider = ? WHERE id = ?";
$stmt = mysqli_prepare($conn, $sql);

if (!$stmt) {
    respond(500, [
        'ok' => false,
        'error' => 'prepare_failed',
        'message' => mysqli_error($conn),
    ]);
}

mysqli_stmt_bind_param($stmt, 'si', $youtubeUrl, $id);

if (!mysqli_stmt_execute($stmt)) {
    respond(500, [
        'ok' => false,
        'error' => 'db_update_failed',
        'message' => mysqli_stmt_error($stmt),
    ]);
}

if (mysqli_stmt_affected_rows($stmt) === 0) {
    respond(404, [
        'ok' => false,
        'error' => 'not_found',
        'message' => 'No row updated. Check id exists.',
        'id' => $id,
    ]);
}

mysqli_stmt_close($stmt);
mysqli_close($conn);

respond(200, [
    'ok' => true,
    'id' => $id,
    'youtube_url' => $youtubeUrl,
]);
