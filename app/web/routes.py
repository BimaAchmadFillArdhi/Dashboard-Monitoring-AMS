from fastapi import APIRouter, Request, Response, Form, Depends, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db.session import get_db_session
from app.db.models import AppUser
from app.services.auth_service import (
    verify_password,
    sign_session_cookie,
    verify_session_cookie,
    ensure_default_users,
)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
settings = get_settings()


async def get_optional_user(request: Request) -> dict | None:
    cookie = request.cookies.get("cctv_session")
    return verify_session_cookie(cookie)


@router.get("/login")
async def login_page(request: Request):
    user = await get_optional_user(request)
    if user:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        request=request,
        name="pages/login.html",
        context={"app_name": settings.APP_NAME, "error": None},
    )


@router.post("/login")
async def login_action(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    session: AsyncSession = Depends(get_db_session),
):
    # Ensure default users exist
    await ensure_default_users(session)

    stmt = select(AppUser).where(AppUser.username == username.strip())
    res = await session.execute(stmt)
    user = res.scalar_one_or_none()

    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse(
            request=request,
            name="pages/login.html",
            context={
                "app_name": settings.APP_NAME,
                "error": "Username atau kata sandi tidak sesuai.",
            },
            status_code=401,
        )

    # Set signed cookie
    session_token = sign_session_cookie(user.id, user.username, user.role)
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(
        key="cctv_session",
        value=session_token,
        httponly=True,
        samesite="lax",
        max_age=7 * 86400,
    )
    return response


@router.get("/logout")
async def logout_action():
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(key="cctv_session")
    return response


@router.get("/")
async def dashboard_page(request: Request):
    user = await get_optional_user(request)
    return templates.TemplateResponse(
        request=request,
        name="pages/dashboard.html",
        context={
            "title": f"Dashboard - {settings.APP_NAME}",
            "page_title": "Dashboard Sistem",
            "active_page": "dashboard",
            "debug": settings.DEBUG,
            "user": user,
        },
    )


@router.get("/applications")
async def applications_page(request: Request):
    user = await get_optional_user(request)
    return templates.TemplateResponse(
        request=request,
        name="pages/applications.html",
        context={
            "title": f"Applications - {settings.APP_NAME}",
            "page_title": "Daftar Aplikasi",
            "active_page": "applications",
            "debug": settings.DEBUG,
            "user": user,
        },
    )


@router.get("/logs")
async def logs_page(request: Request):
    user = await get_optional_user(request)
    return templates.TemplateResponse(
        request=request,
        name="pages/logs.html",
        context={
            "title": f"Logs - {settings.APP_NAME}",
            "page_title": "Log & CCTV Bermasalah",
            "active_page": "logs",
            "debug": settings.DEBUG,
            "user": user,
        },
    )


@router.get("/_styleguide")
async def styleguide_page(request: Request):
    user = await get_optional_user(request)
    return templates.TemplateResponse(
        request=request,
        name="pages/styleguide.html",
        context={
            "title": f"Styleguide - {settings.APP_NAME}",
            "page_title": "Styleguide Sistem Desain",
            "active_page": "styleguide",
            "debug": settings.DEBUG,
            "user": user,
        },
    )
