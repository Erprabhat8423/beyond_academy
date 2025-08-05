"""
Skill extraction module for CV processing using OpenAI
"""
import os
import json
import logging
from typing import List, Dict, Optional
from django.db import transaction

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import PyPDF2
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False

from zoho_app.models import Skill

logger = logging.getLogger(__name__)


class SkillExtractor:
    """Handles skill extraction from CV PDF files using OpenAI"""
    
    def __init__(self):
        """Initialize the skill extractor with OpenAI API key"""
        if not OPENAI_AVAILABLE:
            logger.warning("OpenAI package not installed. Install with: pip install openai")
            raise ImportError("OpenAI package is required for skill extraction")
        
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        if not self.openai_api_key:
            logger.warning("OPENAI_API_KEY not found in environment variables")
            raise ValueError("OpenAI API key is required for skill extraction")
        
        # Initialize OpenAI client (v1.x API)
        try:
            self.client = openai.OpenAI(api_key=self.openai_api_key)
            logger.info("SkillExtractor initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
            # Try with older API format if available
            try:
                openai.api_key = self.openai_api_key
                self.client = None  # Use global openai module
                logger.info("SkillExtractor initialized with legacy OpenAI API")
            except Exception as e2:
                logger.error(f"Failed to initialize legacy OpenAI API: {e2}")
                raise ValueError(f"Could not initialize OpenAI client: {e}")
    
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Extract text content from PDF file
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Extracted text content
        """
        if not PDF_SUPPORT:
            logger.error("PyPDF2 package not installed. Install with: pip install PyPDF2")
            return ""
        
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                
                for page_num, page in enumerate(pdf_reader.pages):
                    try:
                        page_text = page.extract_text()
                        text += page_text + "\n"
                    except Exception as e:
                        logger.warning(f"Error extracting text from page {page_num}: {e}")
                        continue
                
                logger.info(f"Extracted {len(text)} characters from PDF: {pdf_path}")
                return text.strip()
                
        except Exception as e:
            logger.error(f"Error extracting text from PDF {pdf_path}: {e}")
            return ""
    
    def extract_skills_with_openai(self, cv_text: str) -> List[Dict[str, str]]:
        """
        Extract skills from CV text using OpenAI
        
        Args:
            cv_text: Text content of the CV
            
        Returns:
            List of skill dictionaries with name, category, and proficiency
        """
        if not cv_text.strip():
            logger.warning("Empty CV text provided")
            return []
        
        try:
            prompt = f"""
            Analyze the following CV text and extract all technical skills, soft skills, and competencies.
            For each skill, provide:
            1. skill_name: The name of the skill
            2. category: Category (Technical, Programming, Language, Soft Skill, Tool/Software, Domain Knowledge, etc.)
            3. proficiency_level: Estimated proficiency (Beginner, Intermediate, Advanced, Expert) based on context
            
            Format the response as a JSON array of objects with these exact fields: skill_name, category, proficiency_level
            
            CV Text:
            {cv_text[:4000]}  # Limit to first 4000 characters to stay within token limits
            
            Respond with only the JSON array, no additional text.
            """
            
            # Make API call - handle both new and legacy OpenAI APIs
            try:
                if self.client:  # New API (v1.x)
                    response = self.client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "You are an expert HR assistant that extracts skills from CVs. Always respond with valid JSON."},
                            {"role": "user", "content": prompt}
                        ],
                        max_tokens=1500,
                        temperature=0.3
                    )
                    response_text = response.choices[0].message.content.strip()
                else:  # Legacy API
                    response = openai.ChatCompletion.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "You are an expert HR assistant that extracts skills from CVs. Always respond with valid JSON."},
                            {"role": "user", "content": prompt}
                        ],
                        max_tokens=1500,
                        temperature=0.3
                    )
                    response_text = response.choices[0].message.content.strip()
            except Exception as api_error:
                logger.error(f"OpenAI API call failed: {api_error}")
                return []
            logger.info(f"OpenAI response received: {len(response_text)} characters")
            logger.debug(f"OpenAI raw response: {response_text[:500]}...")  # Log first 500 chars for debugging
            
            # Try to parse as JSON
            try:
                skills_data = json.loads(response_text)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse OpenAI response as JSON: {e}")
                logger.debug(f"Raw response: {response_text}")
                # Try to extract JSON from response if it's wrapped in text
                start_idx = response_text.find('[')
                end_idx = response_text.rfind(']') + 1
                if start_idx != -1 and end_idx != 0:
                    try:
                        skills_data = json.loads(response_text[start_idx:end_idx])
                    except json.JSONDecodeError:
                        return []
                else:
                    return []
            
            if not isinstance(skills_data, list):
                logger.error(f"OpenAI response is not a list: {type(skills_data)}")
                return []
            
            # Validate the structure
            valid_skills = []
            for skill in skills_data:
                if isinstance(skill, dict) and all(key in skill for key in ['skill_name', 'category', 'proficiency_level']):
                    # Clean and validate the skill data
                    cleaned_skill = {
                        'skill_name': str(skill['skill_name']).strip()[:255],  # Limit to model field length
                        'category': str(skill['category']).strip()[:100],
                        'proficiency_level': str(skill['proficiency_level']).strip()[:50]
                    }
                    
                    # Only add if skill name is not empty
                    if cleaned_skill['skill_name']:
                        valid_skills.append(cleaned_skill)
                else:
                    logger.warning(f"Invalid skill format: {skill}")
            
            logger.info(f"Successfully extracted {len(valid_skills)} valid skills")
            return valid_skills
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            return []
        except Exception as e:
            logger.error(f"Error extracting skills with OpenAI: {e}")
            return []
    
    def save_skills_to_database(self, skills: List[Dict[str, str]], contact_id: str, document_id: int) -> List[int]:
        """
        Save extracted skills to the database
        
        Args:
            skills: List of skill dictionaries
            contact_id: Zoho contact ID
            document_id: Database document ID
            
        Returns:
            List of created skill IDs
        """
        if not skills:
            logger.info("No skills to save")
            return []
        
        created_skill_ids = []
        
        try:
            with transaction.atomic():
                # Remove existing skills for this document to avoid duplicates
                Skill.objects.filter(contact_id=contact_id, document_id=document_id).delete()
                
                # Create new skills
                for skill_data in skills:
                    try:
                        skill = Skill.objects.create(
                            contact_id=contact_id,
                            document_id=document_id,
                            skill_name=skill_data['skill_name'],
                            skill_category=skill_data['category'],
                            proficiency_level=skill_data['proficiency_level'],
                            extraction_method='openai_gpt3.5',
                            confidence_score=0.8,  # Default confidence for OpenAI extraction
                        )
                        created_skill_ids.append(skill.id)
                        
                    except Exception as e:
                        logger.error(f"Error creating skill {skill_data['skill_name']}: {e}")
                        continue
                
                logger.info(f"Saved {len(created_skill_ids)} skills to database for contact {contact_id}")
                
        except Exception as e:
            logger.error(f"Error saving skills to database: {e}")
            created_skill_ids = []
            
        return created_skill_ids
    
    def extract_and_save_skills(self, pdf_path: str, contact_id: str, document_id: int) -> List[int]:
        """
        Complete workflow: extract text from PDF, extract skills with OpenAI, save to database
        
        Args:
            pdf_path: Path to the PDF file
            contact_id: Zoho contact ID
            document_id: Database document ID
            
        Returns:
            List of created skill IDs
        """
        logger.info(f"Starting skill extraction workflow for {pdf_path}")
        
        # Extract text from PDF
        cv_text = self.extract_text_from_pdf(pdf_path)
        if not cv_text:
            logger.warning(f"No text extracted from PDF: {pdf_path}")
            return []
        
        # Extract skills using OpenAI
        skills = self.extract_skills_with_openai(cv_text)
        if not skills:
            logger.warning(f"No skills extracted from CV text")
            return []
        
        # Save skills to database
        skill_ids = self.save_skills_to_database(skills, contact_id, document_id)
        
        logger.info(f"Completed skill extraction workflow: {len(skill_ids)} skills saved")
        return skill_ids

    def extract_skills_from_text(self, cv_text: str, contact_id: str, document_id: int) -> List[int]:
        """
        Extract skills from already extracted text (useful for non-PDF documents)
        
        Args:
            cv_text: Text content of the CV
            contact_id: Zoho contact ID
            document_id: Database document ID
            
        Returns:
            List of created skill IDs
        """
        logger.info(f"Starting skill extraction from text for contact {contact_id}")
        
        # Extract skills using OpenAI
        skills = self.extract_skills_with_openai(cv_text)
        if not skills:
            logger.warning("No skills extracted from CV text")
            return []
        
        # Save skills to database
        skill_ids = self.save_skills_to_database(skills, contact_id, document_id)
        
        logger.info(f"Completed skill extraction from text: {len(skill_ids)} skills saved")
        return skill_ids
